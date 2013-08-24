open Lwt
open Lwt_log

exception Exited
exception Error

type 'a t = 'a Lwt_stream.t
type ('a,'b) pipe = ('a Lwt_stream.t * ('b option -> unit))

type syn = Syn
type ack = Ack

type 'c master = {
  ofd: 'c;
  tfd: 'c;
  ack: 'c;
  syn: 'c;
  lck: Lwt_mutex.t;
}

let master = ref None

let pid = Unix.getpid ()

let socket_name pid =
  let open FilePath in
  let base = chop_extension (basename Sys.executable_name) in
  let name = Printf.sprintf "%s-%d" base pid in
  let path = make_absolute "/tmp" name in
  Unix.ADDR_UNIX path

let init_master (ofd,tfd,ack,syn) =
  master := Some {ofd;tfd;ack;syn;lck = Lwt_mutex.create ()}

let buffered_io m =
  let of_unix_fd mode fd =
    let fd = Lwt_unix.of_unix_file_descr ~blocking:true fd in
    Lwt_io.of_fd ~mode fd in
  of_unix_fd Lwt_io.input  m.ofd,
  of_unix_fd Lwt_io.output m.tfd,
  of_unix_fd Lwt_io.input  m.ack,
  of_unix_fd Lwt_io.output m.syn


let run_transaction m f =
  let transaction () =
    try_lwt
      let ofd,tfd,ack,syn = buffered_io m in
      lwt ()  = Lwt_io.(write_value syn Syn >> flush syn) in
      lwt Ack = Lwt_io.read_value ack in
      lwt r  = f ofd tfd in
      lwt () = Lwt_io.flush tfd in
      return r
    with exn -> error ~exn "master i/o" >> fail exn in
  Lwt_mutex.with_lock m.lck transaction

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
  let () = try
      Lwt_unix.bind sock (socket_name (Unix.getpid ()))
    with exn -> ign_error ~exn "bind failed" in
  Lwt_unix.listen sock 0;
  lwt fd,addr = Lwt_unix.accept sock in
  work fd

let create_worker exec =
  flush_all ();
  match Lwt_unix.fork () with
    | 0  -> Lwt_main.run (worker_thread exec); exit 0
    | pid -> socket_name pid


let rec reap n =
  try
    match Unix.waitpid [Unix.WNOHANG] (-1) with
    | 0,_ -> ()
    | _   -> reap n
  with Unix.Unix_error (Unix.ECHILD,_,_) -> ()
     | exn -> ign_error ~exn "reap failed"

let create_master () =
  let of_master,to_main = Unix.pipe () in
  let of_main,to_master = Unix.pipe () in
  let syn_req,req_ack   = Unix.pipe () in
  let syn_ack,put_ack   = Unix.pipe () in
  flush_all ();
  match Lwt_unix.fork () with
  | 0 ->
    init_master (of_master,to_master,syn_ack,req_ack);
    Sys.set_signal Sys.sigchld (Sys.Signal_handle reap);
    let of_main = Unix.in_channel_of_descr  of_main in
    let to_main = Unix.out_channel_of_descr to_main in
    let syn_req = Unix.in_channel_of_descr  syn_req in
    let put_ack = Unix.out_channel_of_descr put_ack in
    let rec loop () =
      let () = try
          let Syn  = Marshal.from_channel syn_req in
          Marshal.to_channel put_ack Ack [];
          flush put_ack;
          let proc = (Marshal.from_channel of_main) in
          let addr = create_worker proc in
          Marshal.to_channel to_main addr [];
          flush to_main;
        with End_of_file -> exit 0
           | exn         -> exit 1 in
      loop () in
    loop ()
  | pid ->
    Unix.(List.iter close [of_main; to_main; syn_req; put_ack]);
    of_master,to_master,syn_ack,req_ack


let init () =
  let fds = create_master () in
  init_master fds

let unlink_addr addr =
  try_lwt
    let open Lwt_unix in match addr with
    | ADDR_UNIX filename -> unlink filename
    | ADDR_INET _ -> return_unit
  with exn -> error ~exn "unlink failed"

let open_connection addr =
  let open Unix in
  let rec loop () =
    let fd =
      Lwt_unix.socket (domain_of_sockaddr addr) SOCK_STREAM 0 in
    try_lwt
      lwt () = Lwt_unix.connect fd addr in
      return fd
    with Unix_error (ENOENT,_,_)
       | Unix_error (ECONNREFUSED,_,_) -> Lwt_unix.close fd >> loop ()
       | exn -> error ~exn "cannot open connection" >> fail exn in
  let getfd = loop () >|= (fun fd -> `Socket fd) in
  let timer = Lwt_unix.sleep 5. >> return `Timeout in
  lwt fd = match_lwt getfd <?> timer with
    | `Socket fd -> return fd
    | `Timeout ->
      fail (Unix_error (ETIMEDOUT, "open_connection","timeout")) in
  unlink_addr addr >> return (make_connection fd)

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
