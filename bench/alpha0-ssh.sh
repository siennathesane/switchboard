#!/usr/bin/env bash
# Run a command on alpha-0 through the live Zed SSH ControlMaster socket.
exec ssh -o ControlPath=/var/folders/ps/6dmlb71d5x970q5nb8p21jcm0000gn/T/zed-ssh-session4BpD1d/ssh.sock \
  -o ConnectTimeout=10 sienna@alpha-0.app.alpha.labs.cdg.sunbeam.pt "$@"
