(** Lwt-enabled parallel processing library.

    This library allows running lwt computations in different OS
    processes. E.g.,

    {[
      let () = Parallel.init () (* do it once in the beginning  *)

      (* ... *)
      let puts = Parallel.run Lwt_io.printl in
      (* ... *)
      (* will be printed in a different process *)
      puts "hello" >>= fun () ->

    ]}


    {2 Implementation Details}

    In general, Unix fork(2) and Lwt do not mix well. There are a few
    issues. First, Lwt uses regular threads (like pthreads) to handle
    some system calls, and threads do not play with forks. Next, Lwt
    promises essentially form a DAG of reference cells that will be
    cloned into the child process on a fork, which is not what you
    usually want. Last but not least, every fork will clone the whole
    heap of the current process, which will be result in a time
    consuming data copying the next time the marks and sweep cycle of
    the GC is run (which will trigger copy-on-write as it will mark
    every block).

    The solution is to create a snapshot of the process before it
    starts any lwt-related computations and use this snapshot to fork
    the new processes. I.e., every time we need to fork a process,
    instead of running fork(2) in the current process we send a request
    to the snapshot process which forks a new child and returns the
    AF_UNIX socket address for communicating with this child. The
    function to be executed along with the protocol specifications are
    marshaled via pipe to the snapshot process, where they are copied
    to the new process space during the fork.
*)


(** a process snapshot handler  *)
type snapshot

(** [x io] defines serialization protocol for the type [x]  *)
type 'a io



(** [snapshot ()] creates a new process snapshot.

    Forks a new process from the current state so that it could be
    used later to create new forks.  *)
val snapshot : unit -> snapshot

(** [init ()] creates the default snapshot.

    If no snapshot is provided to [run] or [create] then the snapshot
    created by [init] will be used. *)
val init : unit -> unit

(** {4 Simple interface}  *)

(** [run f] runs [f] in different process.

    @param snapshot uses the snapshot as the fork point, if no
    specified, uses the one created with [init ()], fails if
    [init ()] wasn't run.

    @param inc defines the serialization protocols for the incoming
    (from the subprocess) messages, i.e., for the function result
    type, defaults to the use of marshaling.

    @param out defines the serialization protocols for the outcoming
    (to the subprocess) messages, i.e., for the function argument
    type, defaults to the use of marshaling. *)
val run :
  ?snapshot: snapshot ->
  ?inc: 'inc io ->
  ?out: 'out io ->
  ('out -> 'inc Lwt.t) -> ('out -> 'inc Lwt.t)

(** {4 Expert interface}  *)

(** Type safe pipe, with ['a] read end and ['b] write end*)
type ('a,'b) pipe = 'a Lwt_stream.t * ('b option -> unit)

(** [create process] executes function [process] in other process.

    @param snapshot uses the snapshot as the fork point, if no
    specified, uses the one created with [init ()], fails if
    [init ()] wasn't run.

    @param inc defines the serialization protocols for the incoming
    (from the subprocess) messages, defaults to the use of marshaling.

    @param out defines the serialization protocols for the outcoming
    (to the subprocess) messages, defaults to the use of marshaling. *)
val process :
  ?snapshot: snapshot ->
  ?inc: 'inc io ->
  ?out:'out io ->
  (('out,'inc) pipe -> unit Lwt.t) -> ('inc,'out) pipe



(** Serialization Protocols. *)
module Io : sig

  (** [define ~put ~get] defines a new serialization protocol.

      The [put] function is responsible for writing the messages to
      the channel.
      The [get] function is responsible for reading the messages from
      the channel. *)
  val define :
    put:(Lwt_io.output_channel -> 'a -> unit Lwt.t) ->
    get:(Lwt_io.input_channel -> 'a Lwt.t) -> 'a io

  (** [put io chan x] uses protocol [io] to write [x] to [chan].  *)
  val put : 'a io -> Lwt_io.output_channel -> 'a -> unit Lwt.t

  (** [get io chan] uses protocol [io] to read a message from [chan].  *)
  val get : 'a io -> Lwt_io.input_channel -> 'a Lwt.t

  (** [marshalling io] uses the [Marshal] module to serialize messages.  *)
  val marshaling : 'a io
end
