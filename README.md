# ImpFS

ImpFS is a novel filesystem with some interesting features.


## Related packages

<img src="https://docs.google.com/drawings/d/e/2PACX-1vSqzipIxfOtcWhtSEqcBUpEKPVp1ALtHYyVVBldz7WNP3idcaQTY0iHoLBMf9n4vNMUjDvoIi_gr2gE/pub?w=1034&amp;h=520">

## Quick links

* OCamldocs at <https://tomjridge.github.io/ocamldocs/> (includes main management page)
* Webpage, with demo and OCaml'20 talk: <http://www.tom-ridge.com/filesystems.html>



## Build

To build with docker, just run `docker build .`

To build from command line, consult the Dockerfile (you need to install all the pre-reqs first).



## src/v1

This version implements directories, and file metadata (not file
data); file data is passed through to some underlying
filesystem. There is no garbage collection, free space tracking, or
anything like that (eventually, you will fill up the store, and will
have to copy the directories and metadata to a new store; but since a
store is just a file this is not too onerous).

This code is not really intended to be used. However, it might be
useful if you want to store metadata and directories on a fast store
(eg SSD), whilst the file data resides on a slower medium (HDD). Our
purpose here is to implement a "minimal" example filesystem (or, at
least, a partial filesystem).

To run, type `make run_v1`. See the MiniFS documentation for more
details on the FUSE flags.



## src/v2

This is the current version of ImpFS.

