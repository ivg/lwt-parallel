open Lwt
open Lwt.Syntax


type to_sub = [ `Start of int * int array | `Stop ]
type of_sub = float

let to_sub : to_sub Parallel.io =
  Parallel.Io.define
    ~put:(fun chan -> function
        | `Stop ->
          Lwt_io.write_line chan "stop"
        | `Start (name,seed) ->
          Lwt_io.write_line chan "start" >>= fun () ->
          Lwt_io.write_int chan name >>= fun () ->
          Lwt_io.write_int chan (Array.length seed) >>= fun () ->
          Array.to_list seed |>
          Lwt_list.iter_s (Lwt_io.write_int chan))
    ~get:(fun chan ->
        Lwt_io.read_line chan >>= function
        | "stop" -> Lwt.return `Stop
        | "start" ->
          let* name = Lwt_io.read_int chan in
          let* seed_size = Lwt_io.read_int chan in
          let seed = Array.make seed_size 0 in
          List.init seed_size (fun x -> x)|>
          Lwt_list.iter_s (fun i ->
              let+ x = Lwt_io.read_int chan in
              seed.(i) <- x) >|= fun () ->
          `Start (name, seed)
        | _ -> failwith "unknown command")

let of_sub = Parallel.Io.define
    ~put:Lwt_io.write_float64
    ~get:Lwt_io.read_float64

let setup_log level =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let seed = [| 7; 8; 42; 56 |]
let tasks = 256
let task_size = 4096 * 1024
let delay = 4. *. atan 1.

let task (data,push) =
  Lwt_stream.next data >>= function
  | `Start (name,seed) ->
    let state = Random.State.make seed in
    Logs_lwt.debug (fun m -> m "<task %03d>: started" name) >>= fun () ->
    Lwt_unix.sleep (Random.float delay) >>= fun () ->
    let array = Array.init task_size (fun _ -> Random.State.float state 1.0) in
    let res = Float.abs @@ Array.fold_left
        (fun acc v -> if Random.State.bool state
          then sin (acc *. v) else cos (acc /. v))
        1.0 array *. 1e5 in
    Logs_lwt.debug (fun m -> m "<task %03d>: computed %g" name res) >>= fun () ->
    return (push (Some res))
  | `Stop -> return (push None)

let spawn_task (name,time) =
  Lwt_unix.sleep time >>= fun () ->
  let result,command =
    Parallel.process ~out:to_sub ~inc:of_sub task in
  command (Some (`Start (name,seed)));
  command (Some `Stop);
  Lwt_stream.get result

let main_dispatcher () =
  let delays = Array.to_list (Array.init tasks (fun i ->
      i,Random.float delay)) in
  Lwt_list.map_p spawn_task delays >>= function
  | Some r :: rs -> return (List.for_all (fun r' -> Some r = r') rs)
  | _ -> return_false



let () =
  setup_log (Some Debug);
  Parallel.init ();
  Lwt_main.run (main_dispatcher () >|= fun r -> assert r)
