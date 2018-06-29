# NOTE maybe add a "RUN echo ..." to force a rebuild, or --no-cache

# maintained in a github repo
FROM impfs_base_docker_image 

RUN echo rebuild

# ----------------------------------------------------------------------

RUN git clone https://github.com/tomjridge/tjr_lib.git
RUN eval `opam config env` && make -C tjr_lib


# ----------------------------------------------------------------------

RUN git clone https://github.com/tomjridge/isa_btree.git 
RUN eval `opam config env` && make -C isa_btree


RUN git clone https://github.com/tomjridge/tjr_monad.git 
RUN eval `opam config env` && make -C tjr_monad


RUN git clone https://github.com/tomjridge/tjr_fs_shared.git 
RUN eval `opam config env` && make -C tjr_fs_shared


# ----------------------------------------------------------------------


RUN git clone -b dev https://github.com/tomjridge/tjr_btree.git
RUN eval `opam config env` && make -C tjr_btree 

RUN git clone https://github.com/tomjridge/path_resolution.git
RUN eval `opam config env` && make -C path_resolution


RUN git clone https://github.com/tomjridge/tjr_net.git
RUN eval `opam config env` && make -C tjr_net


# ----------------------------------------------------------------------

RUN git clone https://github.com/tomjridge/tjr_pcache.git
RUN eval `opam config env` && make -C tjr_pcache && make -C tjr_pcache check

# FIXME perhaps we want to run checks at the end?



RUN git clone -b dev https://github.com/tomjridge/mini-fs.git
RUN eval `opam config env` && make -C mini-fs


# NOTE this is currently private, so we can't just clone from github
# RUN git clone https://github.com/tomjridge/imp_fs.git
# RUN eval `opam config env` && make -C imp_fs
 



# docker build -t some_name .
# docker run -ti some_name bash
