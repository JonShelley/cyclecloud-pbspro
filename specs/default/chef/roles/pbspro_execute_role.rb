name "pbspro_execute_role"
description "PBSPro Execute Role"
run_list("recipe[cshared::client]",
  "recipe[cuser]",
  "recipe[pbspro::execute]",
  "recipe[cganglia::client]")
