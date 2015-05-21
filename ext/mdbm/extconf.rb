# Loads mkmf which is used to make makefiles for Ruby extensions
require 'mkmf'

# Give it a name
extension_name = 'mdbm'

# The destination
dir_config(extension_name)

$CFLAGS = "-I/usr/local/include"

if find_library("mdbm", "mdbm_open", "/tmp/install/lib64",  "/tmp/install/lib")
then
  create_makefile(extension_name)
else
  puts "No mdbm lib found"
end


create_makefile(extension_name)
