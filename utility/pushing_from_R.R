# Using git2r to push commits

library(git2r)

# make changes in the commit

homedir <- "C:/Users/daniel.flynn.ctr"

working.repo <- init(file.path(homedir, "git/RIEM-CIEM"))
config(working.repo, user.name = "dflynn-volpe", user.email = "daniel.flynn.ctr@dot.gov")
cred <- cred_ssh_key(publickey = file.path(homedir, ".ssh/github_rsa.pub"),
             privatekey = file.path(homedir, ".ssh/github_rsa"))

git2r::add(working.repo, ".")
commit(working.repo, "test from git2r")
push(working.repo, 'origin', 'master', credentials = cred)
