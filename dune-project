(lang dune 2.8)
(name tjr_impfs)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/imp_fs))
(homepage             https://github.com/tomjridge/imp_fs)
(documentation        https://tomjridge.github.io/ocamldocs)
(generate_opam_files  true)


(package 
 (name tjr_impfs)
 (synopsis "ImpFS, a new filesystem")
 (depends 
   bigstringaf
   tjr_kv
   tjr_minifs))

