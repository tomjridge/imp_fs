# NOTE maybe add a "RUN echo ..." to force a rebuild, or --no-cache

# FROM ocaml/opam:ubuntu - latest doesn't work due to problems with ppx_deriving deps
# 17.04 doesn't work due to missing archives at ubuntu? or stale packages info?
FROM ocaml/opam:ubuntu-16.04_ocaml-4.04.2
# RUN apt-get -y update

RUN opam list
RUN opam update
RUN opam install batteries extunix
RUN opam install ppx_deriving_yojson extlib core ppx_bin_prot ocaml-compiler-libs lwt
RUN opam list
RUN ocamlfind list


RUN git clone https://github.com/tomjridge/isa_btree.git 
RUN eval `opam config env` && make -C isa_btree

RUN git clone https://github.com/tomjridge/tjr_monad.git 
RUN eval `opam config env` && make -C tjr_monad

RUN git clone https://github.com/tomjridge/tjr_fs_shared.git 
RUN eval `opam config env` && make -C tjr_fs_shared

RUN git clone -b dev https://github.com/tomjridge/tjr_btree.git
RUN eval `opam config env` && make -C tjr_btree 



RUN git clone https://github.com/tomjridge/path_resolution.git
RUN eval `opam config env` && make -C path_resolution


RUN git clone https://github.com/tomjridge/tjr_net.git
RUN eval `opam config env` && make -C tjr_net


# for tjr_lib
RUN opam install core_kernel ppx_deriving_yojson rresult bos fileutils omd

RUN git clone https://github.com/tomjridge/tjr_lib.git
RUN eval `opam config env` && make -C tjr_lib


# for mini-fs
RUN sudo apt-get -y install libfuse-dev

RUN opam install ocamlfuse ppx_bin_prot 
RUN git clone -b dev https://github.com/tomjridge/mini-fs.git
RUN eval `opam config env` && make -C mini-fs


# NOTE this is currently private, so we can't just clone from github
# RUN git clone https://github.com/tomjridge/imp_fs.git
# RUN eval `opam config env` && make -C imp_fs
 



# docker build -t some_name .
# docker run -ti some_name bash
