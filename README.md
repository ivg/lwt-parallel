Lwt-enabled Parallel Processing Library
=======================================

This library allows running lwt computations in different OS processes. E.g.,

```ocaml
(* do it once in the beginning  *)
let () = Parallel.init ()

(* ... *)
let puts = Parallel.run Lwt_io.printl in

(* will be printed in a different process *)
puts "hello" >>= fun () ->
```

Implementation Details
----------------------

In general, Unix fork(2) and Lwt do not mix well. There are a few issues. First, Lwt uses regular threads (like pthreads) to handle some system calls, and threads do not play with forks. Next, Lwt promises essentially form a DAG of reference cells that will be cloned into the child process on a fork so that the promises made in parent will be triggered in the child, which is not what you usually want. Last but not least, every fork will clone the whole heap of the current process, which will be result in a time consuming data copying the next time the marks and sweep cycle of the GC is run (which will trigger copy-on-write as it will mark every block).

The solution is to create a snapshot of the process before it starts any lwt-related computations and use this snapshot to fork the new processes. I.e., every time we need to fork a process, instead of running fork(2) in the current process we send a request to the snapshot process which forks a new child and returns the AF_UNIX socket address for communicating with this child. The function to be executed along with the protocol specifications are marshaled via pipe to the snapshot process, where they are copied to the new process space during the fork.
