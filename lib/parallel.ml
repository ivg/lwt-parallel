open Lwt

exception Exited
exception Error

type 'a t = 'a Lwt_stream.t
type ('a,'b) pipe = ('a Lwt_stream.t * ('b option -> unit))

let snapshots = ref 0

let make_name ?(suffix="") pid =
  let open Filename in
  let base = basename Sys.executable_name in
  let base =
    try chop_extension base with Invalid_argument _ -> base in
  let name =
    Format.asprintf "%s-%d-%d%s" base pid !snapshots suffix in
  let tmp = Filename.get_temp_dir_name () in
  concat tmp name

let socket_name pid = Unix.ADDR_UNIX (make_name pid)

let bind_socket = Lwt_unix.Versioned.bind_2 [@warning "-3"]


module Mutex : sig
  type t
  val create: int -> t
  val with_lock: t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
  val remove : t -> unit
end = struct
  type t = {
    p_guard : Lwt_unix.file_descr;
    t_guard : Lwt_mutex.t;
    path : string;
  }


  let create pid =
    let name = make_name ~suffix:".lock" pid in
    let fd = Unix.(openfile name [O_WRONLY; O_CREAT] 0o200) in
    {
      p_guard = Lwt_unix.of_unix_file_descr fd;
      t_guard = Lwt_mutex.create ();
      path = name;
    }

  let lock_p fd = Lwt_unix.(lockf fd F_LOCK 1)
  let unlock_p fd = Lwt_unix.(lockf fd F_ULOCK 1)

  let with_p_lock fd f =
    lock_p fd >>= fun () ->
    try_bind f
      (fun r -> unlock_p fd >>= fun () -> return r)
      (fun exn -> unlock_p fd >>= fun () -> fail exn)

  let with_lock m f =
    Lwt_mutex.with_lock m.t_guard (fun () -> with_p_lock m.p_guard f)

  let remove {path} =
    if Sys.file_exists path then Unix.unlink path
end

type snapshot = {
  cld: int;
  ofd: Unix.file_descr;
  tfd: Unix.file_descr;
  lck: Mutex.t;
}

let master = ref None

let error ~exn msg =
  Logs_lwt.err (fun m -> m "%s: %s" msg (Printexc.to_string exn))

let ign_error ~exn msg =
  error ~exn msg |> ignore

let rec reap n =
  try
    match Unix.waitpid [Unix.WNOHANG] (-1) with
    | 0,_ -> ()
    | _   -> reap n
  with Unix.Unix_error (Unix.ECHILD,_,_) -> ()
     | exn -> Logs.err (fun m -> m "reap failed")

let cleanup {ofd; tfd; cld; lck} =
  Unix.close ofd;
  Unix.close tfd;
  Unix.kill cld Sys.sigterm;
  Mutex.remove lck


let create_snapshot ofd tfd pid =
  incr snapshots;
  {ofd; tfd; lck=Mutex.create pid; cld=pid}

let buffered_io m =
  let of_unix_fd mode fd =
    let fd = Lwt_unix.of_unix_file_descr ~blocking:true fd in
    Lwt_io.of_fd ~mode fd in
  of_unix_fd Lwt_io.input  m.ofd,
  of_unix_fd Lwt_io.output m.tfd

let run_transaction m f =
  let transaction () =
    let ofd,tfd = buffered_io m in
    try_bind (fun () -> f ofd tfd)
      return
      (fun exn ->
         error ~exn "master i/o" >>= fun () -> fail exn) in
  Mutex.with_lock m.lck transaction

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
      Lwt_io.close self#wo >>= fun () ->
      Lwt_io.close self#ro >>= fun () ->
      Lwt_unix.close fd
  end

type 'a io = {
  put : Lwt_io.output_channel -> 'a -> unit Lwt.t;
  get : Lwt_io.input_channel -> 'a Lwt.t
}

let marshaling : 'a io = {
  put = Lwt_io.write_value
      ~flags:[Marshal.Closures];
  get = Lwt_io.read_value;
}

module Io = struct
  let define ~put ~get = {put; get}
  let put io = io.put
  let get io = io.get
  let marshaling = marshaling
end

let write io fd v =
  try_bind
    (fun () -> io.put fd v)
    (fun () -> Lwt_io.flush fd)
    (fun exn -> error ~exn "write failed")

let worker_thread inc out exec =
  let recv of_fd push =
    let rec loop () =
      out.get of_fd >>= fun a ->
      push (Some a);
      loop () in
    catch loop
      (function
        | End_of_file -> return_unit
        | exn  -> error ~exn "Process exited with") >>= fun () ->
    return (push None) in
  let send to_fd of_stream =
    catch
      (fun () -> Lwt_stream.iter_s (write inc to_fd) of_stream)
      (fun exn -> error ~exn "parallel write failed") in
  let work fd =
    let conn = make_connection fd in
    let astream,apush = Lwt_stream.create () in
    let bstream,bpush = Lwt_stream.create () in
    let recv_t = recv conn#ro apush >>= fun () -> conn#read_finished in
    let send_t = send conn#wo bstream >>= fun () -> conn#write_finished in
    let exec_t = exec (astream,bpush) in
    async (fun () -> (recv_t <&> send_t) >>= fun () -> conn#close);
    exec_t in
  let sock = Lwt_unix.(socket PF_UNIX SOCK_STREAM 0) in
  Lwt.catch (fun () ->
      bind_socket sock (socket_name (Unix.getpid ())))
    (fun exn -> error ~exn "bind failed") >>= fun () ->
  Lwt_unix.listen sock 0;
  Lwt_unix.accept sock >>= fun (fd,addr) ->
  work fd

let create_worker inc out exec =
  flush_all ();
  match Lwt_unix.fork () with
  | 0  -> exit (Lwt_main.run (
      catch (fun () -> worker_thread inc out exec)
        (fun exn -> error ~exn "Subprocess failed" >>= fun () -> return 1)))

  | pid -> socket_name pid

let snapshot () =
  let of_master,to_main = Unix.pipe () in
  let of_main,to_master = Unix.pipe () in
  flush_all ();
  match Unix.fork () with
  | 0 ->
    Sys.set_signal Sys.sigchld (Sys.Signal_handle reap);
    let of_main = Unix.in_channel_of_descr  of_main in
    let to_main = Unix.out_channel_of_descr to_main in
    let rec loop () =
      let () = try
          let proc = Marshal.from_channel of_main in
          let inc = Marshal.from_channel of_main in
          let out = Marshal.from_channel of_main in
          let addr = create_worker inc out proc in
          Marshal.to_channel to_main addr [];
          flush to_main;
        with End_of_file -> exit 0
           | exn -> exit 1 in
      loop () in
    loop ()
  | pid ->
    let snapshot = create_snapshot of_master to_master pid in
    at_exit (fun () -> cleanup snapshot);
    Unix.(List.iter close [of_main; to_main]);
    snapshot


let init () =
  master := Some (snapshot ())

let unlink_addr addr = match addr with
  | Unix.ADDR_UNIX filename ->
    catch (fun () -> Lwt_unix.unlink filename)
      (fun exn -> error ~exn "unlink failed")
  | Unix.ADDR_INET _ -> return_unit

let open_connection addr =
  let open Unix in
  let rec loop () =
    let fd =
      Lwt_unix.socket (domain_of_sockaddr addr) SOCK_STREAM 0 in
    catch
      (fun () -> Lwt_unix.connect fd addr >>= fun () -> return fd)
      (function
        | Unix_error (ENOENT,_,_)
        | Unix_error (ECONNREFUSED,_,_) ->
          Lwt_unix.close fd >>= fun () -> loop ()
        | exn -> error ~exn "cannot open connection" >>= fun () -> fail exn) in
  let getfd = loop () >|= (fun fd -> `Socket fd) in
  let timer = Lwt_unix.sleep 5. >>= fun () -> return `Timeout in

  let fd = (getfd <?> timer) >>= function
    | `Socket fd -> return fd
    | `Timeout ->
      fail (Unix_error (ETIMEDOUT, "open_connection","timeout")) in
  fd >>= fun fd -> unlink_addr addr >>= fun () ->
  return (make_connection fd)

let create_client inc out f master =
  let astream,apush = Lwt_stream.create () in
  let bstream,bpush = Lwt_stream.create () in
  let io_thread () =
    let puller ro_chan =
      let rec loop () =
        inc.get ro_chan >>= fun b ->
        bpush (Some b);
        loop () in
      catch loop (function
          | End_of_file -> return (bpush None)
          | exn -> bpush None; error ~exn "Client died unexpectedly") in
    let pusher wo_chan =
      catch
        (fun () -> Lwt_stream.iter_s (write out wo_chan) astream)
        (fun exn -> error ~exn "parallel task failed") in
    let connect_to_client () =
      run_transaction master (fun of_master to_master ->
          write marshaling to_master f >>= fun () ->
          write marshaling to_master inc >>= fun () ->
          write marshaling to_master out >>= fun () ->
          Lwt_io.read_value of_master >>= fun addr ->
          open_connection addr) in
    connect_to_client () >>= fun chan ->
    let pull_t = puller chan#ro >>= fun () -> chan#read_finished  in
    let push_t = pusher chan#wo >>= fun () -> chan#write_finished in
    (pull_t <&> push_t) >>= fun () -> chan#close in
  async io_thread;
  bstream,apush

let process ?snapshot ?(inc=marshaling) ?(out=marshaling) f =
  match snapshot, !master with
  | Some t,_ | _, Some t -> create_client inc out f t
  | None,None -> failwith "Parallel: either specify a snapshot or run init"

let run ?snapshot ?inc ?out exec =
  let rec child_f (astream,bpush) =
    let bs = Lwt_stream.map_s exec astream in
    Lwt_stream.iter (fun b -> bpush (Some b)) bs in
  let stream,push = process ?snapshot ?inc ?out child_f in
  let master_f a =
    push (Some a);
    Lwt_stream.next stream in
  master_f
