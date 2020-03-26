(lang dune 2.0)
(name tjr_imp_fs)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/imp_fs))
(homepage             https://github.com/tomjridge/imp_fs)
(documentation        https://tomjridge.github.io/imp_fs)
(generate_opam_files  true)


(package 
 (name tjr_imp_fs)
 (synopsis "ImpFS, a new filesystem")
 (depends 
   tjr_kv
   tjr_minifs))

(package 
 (name tjr_mini_impfs)
 (synopsis "ImpFS, a new filesystem (mini version)")
 (depends 
   tjr_kv
   tjr_minifs))


(package 
 (name tjr_impfs_v1)
 (synopsis "ImpFS, a new filesystem (v1 version)")
 (depends 
   tjr_kv
   tjr_minifs))

