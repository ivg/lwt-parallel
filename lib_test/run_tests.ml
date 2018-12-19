open Lwt

let setup_log level =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let _ =
  setup_log (Some Debug);
  Parallel.init ()

let seed = [| 7; 8; 42; 56 |]
let tasks = 128
let task_size = 4096

let task (data,push) =
  Lwt_stream.next data >>= function
  | `Start state ->
    Lwt_unix.sleep (Random.float 4.) >>= fun ()->
    let array = Array.init task_size (fun _ -> Random.State.int state 100) in
    let res = Array.fold_left
      (fun acc v -> if Random.State.bool state then acc + v else acc - v )
      0 array in
    return (push (Some res))
  | `Stop -> return (push None)

let spawn_task time =
  Lwt_unix.sleep time >>= fun () ->
  let state = Random.State.make seed in
  let result,command = Parallel.process task in
  command (Some (`Start state));
  command (Some `Stop);
  Lwt_stream.get result

let main_dispatcher () =
  let delays = Array.to_list (Array.init tasks (fun _ -> Random.float 4.)) in
  Lwt_list.map_p spawn_task delays >>= function
  | Some r :: rs -> return (List.for_all (fun r' -> Some r = r') rs)
  | _ -> return_false

let _ = Lwt_main.run (main_dispatcher () >>= Lwt_io.printf "%b")
