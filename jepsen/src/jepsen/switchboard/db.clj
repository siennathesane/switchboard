(ns jepsen.switchboard.db
  "Control of switchboard broker processes over SSH. Nodes are pre-baked
  containers (see jepsen/docker): each runs sshd, a broker watchdog, and
  the switchboard binary; sb-start takes explicit flags (sshd sessions do
  not inherit container environment). Jepsen starts and stops broker
  processes; containers stay up so a SIGKILL never destroys the node, and
  the container watchdog restarts the broker whenever it dies or wedges
  (except while Jepsen has deliberately disarmed it for a kill)."
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]]
            [clj-commons.slingshot :refer [try+]]))

(def data-dir  "/var/lib/switchboard")
(def log-file  "/var/log/switchboard/broker.log")
(def pid-file  "/var/run/switchboard.pid")
(def enable-flag "/var/run/sb-enabled")
(def start-cmd "/usr/local/bin/sb-start")
(def client-port 5672)

(defn- stop-broker!
  "SIGTERM, then SIGKILL, ignoring 'no such process'."
  []
  (c/exec "sh" "-c"
          "pkill -x switchboard; sleep 0.5; pkill -9 -x switchboard; exit 0"))

(defn- disarm-watchdog!
  "Disarms the node's broker watchdog (a kill! must keep the node down)."
  []
  (c/exec "sh" "-c" (str "rm -f " enable-flag)))

(defn- start-broker!
  "Starts the broker via sb-start with explicit flags. Node ids are the
  1-based position in the test's node list (stable across restarts, which
  the join protocol requires); the advertised address is the container
  hostname. The first node bootstraps; everyone else joins through it.
  Also (re-)arms the container watchdog, which restarts the broker if it
  dies or wedges, using the persisted args."
  [test node]
  (let [nodes      (vec (:nodes test))
        node-id    (inc (.indexOf ^java.util.List nodes node))
        first-node (first nodes)
        extra      (if (= node first-node)
                     ["--bootstrap"]
                     ["--seeds" (str first-node ":5673")])
        args       (concat [(str node-id) node (str (count nodes))] extra)]
    (c/exec "sh" "-c"
            (str "printf '%s\\n' " (str/join " " (map #(str "'" % "'") args))
                 " > /var/run/sb-args"))
    ;; Start BEFORE arming the watchdog: the watchdog judges liveness by
    ;; pgrep, so arming first lets its next tick race this very start and
    ;; spawn a second broker (the loser dies on the RocksDB lock).
    (apply c/exec* start-cmd (str node-id) node (str (count nodes)) extra)
    (c/exec "sh" "-c" (str "touch " enable-flag))))

(defn await-port!
  "Waits until this node's client port accepts TCP."
  ([]
   (await-port! 120))
  ([timeout-s]
   (c/exec "sh" "-c"
           (str "for i in $(seq 1 " (* 2 timeout-s) "); do "
                "nc -z 127.0.0.1 " client-port " >/dev/null 2>&1 && exit 0; "
                "sleep 0.5; done; "
                "echo 'broker client port never came up' >&2; exit 1"))))

(defn- await-start!
  "Waits for the just-started broker to serve. Returns :up, :died (process
  exited — e.g. it lost the RocksDB lock race and must be restarted), or
  :slow (still starting; a broker replaying a large raft log must NEVER be
  killed for being slow, so the caller leaves it alone)."
  []
  (let [code (c/exec "sh" "-c"
                     (str "for i in $(seq 1 40); do "
                          "nc -z 127.0.0.1 " client-port " >/dev/null 2>&1 && exit 0; "
                          "pgrep -x switchboard >/dev/null 2>&1 || exit 2; "
                          "sleep 0.5; done; exit 3"))]
    (case (str/trim code)
      "0" :up
      "2" :died
      :slow)))

(defrecord DB []
  db/DB
  (setup! [_ test node]
    (try+ (stop-broker!) (catch Object _))
    ;; Wipe state only during initial cluster setup; kill/start nemesis
    ;; events deliberately preserve the data dir (that's the point).
    (c/exec "rm" "-rf" data-dir)
    (c/exec "sh" "-c" (str "rm -f " log-file " " pid-file))
    (start-broker! test node)
    (await-port! 120))

  (teardown! [_ _ _]
    (try+ (disarm-watchdog!) (catch Object _))
    (try+ (stop-broker!) (catch Object _)))

  db/Kill
  (kill! [_ _ _]
    (info "Killing switchboard")
    ;; Disarm first so the container watchdog doesn't immediately undo
    ;; the kill; start! re-arms it.
    (disarm-watchdog!)
    (c/exec "sh" "-c" "pkill -9 -x switchboard; exit 0"))

  (start! [_ test node]
    (info "Starting switchboard")
    ;; The container watchdog restarts the broker once re-armed; all we do
    ;; here is arm it and wait for the port.
    (start-broker! test node)
    (await-port! 30))

  db/Pause
  (pause! [_ _ _]
    (info "Pausing switchboard (SIGSTOP)")
    ;; By name, not by the pidfile: the pidfile can name a broker that
    ;; lost a start race; the pause must hit the broker that runs.
    (c/exec "sh" "-c" "pkill -STOP -x switchboard; exit 0"))

  (resume! [_ _ _]
    (info "Resuming switchboard (SIGCONT)")
    (c/exec "sh" "-c" "pkill -CONT -x switchboard; exit 0"))

  db/LogFiles
  (log-files [_ _ _]
    {log-file "broker.log"}))

(defn db
  "A switchboard cluster."
  []
  (DB.))
