(** Summary of main interfaces *)

(** {2 Usedlist interface} 

{[
$(INCLUDE("common/GEN.usedlist*.ml_"))
]}

*)



(** {2 File interface} 

{[
$(INCLUDE("common/GEN.file*.ml_"))
]}

{v

$(INCLUDE("common/fv2.md"))

v}

*)


(** {2 Dir interface} 

{[
$(INCLUDE("common/GEN.dir*.ml_"))
]}

*)



(** {2 V1 interfaces}


{[
$(INCLUDE("v1/GEN*"))
]}

See also {!V1_generic.Make.Make_2} (abstract ops) and {!V1_generic.Make.Make_3} (with restricted sig) and {!V1.With_gom.The_filesystem} (the actual impl we use)
*)
