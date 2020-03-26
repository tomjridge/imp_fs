# ImpFS

ImpFS is a novel filesystem with some interesting features.


## Related packages

<img src="https://docs.google.com/drawings/d/e/2PACX-1vSqzipIxfOtcWhtSEqcBUpEKPVp1ALtHYyVVBldz7WNP3idcaQTY0iHoLBMf9n4vNMUjDvoIi_gr2gE/pub?w=1034&amp;h=520">


## src-mini

This contains a prototype filesystem to test the design etc.


## src-v1

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
