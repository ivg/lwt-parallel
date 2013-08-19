open Lwt
open Lwt_log

exception Exited
exception Error

type 'a t = 'a Lwt_stream.t
type ('a,'b) pipe = ('a Lwt_stream.t * ('b option -> unit))


let master = ref None

let init_master ofd tfd =
  master := Some (ofd,tfd,Lwt_mutex.create ())

let run_transaction (ofd,tfd,guard) f =
  let of_unix_fd = Lwt_unix.of_unix_file_descr ~blocking:true in
  let ofd,tfd = of_unix_fd ofd, of_unix_fd tfd in
  let lock () =
    Lwt_unix.(lockf tfd F_LOCK 0 >> lockf ofd F_RLOCK 0) in
  let unlock () =
    Lwt_unix.(lockf tfd F_ULOCK 0 >> lockf ofd F_ULOCK 0) in
  try_lwt
    let of_master = Lwt_io.(of_fd  ~mode:input  ofd) in
    let to_master = Lwt_io.(of_fd  ~mode:output tfd) in
    lwt () = lock () in
    lwt r = Lwt_mutex.with_lock guard (fun () -> f of_master to_master) in
    lwt () = Lwt_io.flush to_master in
    lwt () = unlock () in
    return r
  with exn -> error ~exn "master i/o" >> unlock () >> fail exn


let file_name pid = Printf.sprintf "/tmp/parallax-%d" pid
let socket_name pid = Unix.ADDR_UNIX (file_name pid)

let shutdown fd cmd =
  try
    Lwt_unix.shutdown fd cmd
  with Unix.Unix_error (Unix.ENOTCONN,_,_) -> ()
     | exn -> ign_error ~exn "shutdown failed"

let make_connection fd =
  let close () = return_unit in
  object(self)
    method ro = Lwt_io.of_fd ~close ~mode:Lwt_io.input fd
    method wo = Lwt_io.of_fd ~close ~mode:Lwt_io.output fd
    method write_finished =
      shutdown fd Unix.SHUTDOWN_SEND;
      return_unit
    method read_finished  =
      shutdown fd Unix.SHUTDOWN_RECEIVE;
      return_unit
    method close =
      Lwt_io.close self#wo >> Lwt_io.close self#ro >>
      Lwt_unix.close fd
  end

let write_value fd v =
  try_lwt
    Lwt_io.write_value ~flags:[Marshal.Closures] fd v
    >> Lwt_io.flush_all ()
  with exn -> error ~exn "write_value failed"

let worker_thread exec =
  let recv of_fd push =
    let rec loop () =
      lwt a = Lwt_io.read_value of_fd in
      push (Some a);
      loop () in
    try_lwt loop ()
    with End_of_file -> return_unit
       | exn  -> error ~exn "Process exited with"
    finally return (push None) in
  let send to_fd of_stream =
    Lwt_stream.iter_s (write_value to_fd) of_stream in
  let work fd =
    let conn = make_connection fd in
    let astream,apush = Lwt_stream.create () in
    let bstream,bpush = Lwt_stream.create () in
    let recv_t = recv conn#ro apush >> conn#read_finished in
    let send_t = send conn#wo bstream >> conn#write_finished in
    let exec_t = exec (astream,bpush) in
    async (fun () -> (recv_t <&> send_t) >> conn#close);
    exec_t in
  let sock = Lwt_unix.(socket PF_UNIX SOCK_STREAM 0) in
  Lwt_unix.bind sock (socket_name (Unix.getpid ()));
  Lwt_unix.listen sock 0;
  lwt fd,addr = Lwt_unix.accept sock in
  work fd

let create_worker exec =
  flush_all ();
  match Lwt_unix.fork () with
    | 0  -> Lwt_main.run (worker_thread exec); exit 0
    | pid -> socket_name pid


let reap n = ignore (Unix.wait ())

let create_master () =
  let of_master,to_main = Unix.pipe () in
  let of_main,to_master = Unix.pipe () in
  flush_all ();
  match Lwt_unix.fork () with
  | 0 ->
    init_master of_master to_master;
    Sys.set_signal Sys.sigchld (Sys.Signal_handle reap);
    let of_main = Unix.in_channel_of_descr  of_main in
    let to_main = Unix.out_channel_of_descr to_main in
    let rec loop () =
      let () = try
          let proc = (Marshal.from_channel of_main) in
          let addr = create_worker proc in
          Marshal.to_channel to_main addr [];
          flush_all ();
        with End_of_file -> exit 0
           | exn         -> exit 1 in
      loop () in
    loop ()
  | pid ->
    Unix.(List.iter close [of_main; to_main]);
    of_master,to_master

let init () =
  let of_master, to_master = create_master () in
  init_master of_master to_master

let unlink addr =
  try_lwt
    let open Lwt_unix in match addr with
    | ADDR_UNIX filename -> unlink filename
    | ADDR_INET _ -> return_unit
  with exn -> error ~exn "unlink failed"

let open_connection addr =
  let rec loop () =
    let fd = Lwt_unix.socket
        (Unix.domain_of_sockaddr addr) Unix.SOCK_STREAM 0 in
    try_lwt
      lwt () = Lwt_unix.connect fd addr in
      return fd
    with exn ->
      Lwt_unix.close fd >> loop () in
  lwt fd = loop () in
  unlink addr >> return (make_connection fd)

let create_client f io =
  let astream,apush = Lwt_stream.create () in
  let bstream,bpush = Lwt_stream.create () in
  let io_thread () =
    let puller ro_chan =
      let rec loop () =
        lwt b = Lwt_io.read_value ro_chan in
        bpush (Some b);
        loop () in
      try_lwt loop () with
      | End_of_file -> return_unit
      | exn -> error ~exn "Client died unexpectedly"
      finally return (bpush None) in
    let pusher wo_chan =
      Lwt_stream.iter_s (write_value wo_chan) astream in
    let connect_to_client () =
      run_transaction io (fun of_master to_master ->
          lwt () = write_value to_master f in
          lwt addr = Lwt_io.read_value of_master in
          open_connection addr) in
    lwt chan = connect_to_client () in
    let pull_t = puller chan#ro >> chan#read_finished  in
      let push_t = pusher chan#wo >> chan#write_finished in
    (pull_t <&> push_t) >> chan#close in
  async io_thread;
  bstream,apush

let process f = match !master with
  | Some t -> create_client f t
  | None -> failwith "Multiprocessor"

let run exec =
  let rec child_f (astream,bpush) =
    let bs = Lwt_stream.map_s exec astream in
    Lwt_stream.iter (fun b -> bpush (Some b)) bs in
  let stream,push = process child_f in
  let guard = Lwt_mutex.create () in
  let master_f a =
    Lwt_mutex.with_lock guard (fun () ->
        push (Some a);
        Lwt_stream.next stream) in
  master_f
