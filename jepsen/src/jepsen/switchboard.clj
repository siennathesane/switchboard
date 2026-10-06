(ns jepsen.switchboard
  "Entry point: parses CLI options and runs the requested tests."
  (:require [clojure [string :as str]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer [info]]
            [jepsen [core :as core]]
            [jepsen.switchboard.tests :as tests])
  (:gen-class))

(def cli-opts
  [[nil "--nodes NODES" "Comma-separated node hostnames"
    :default "sb1,sb2,sb3,sb4,sb5"]
   ["-t" "--test TEST" "Test(s) to run: fifo, fanout, topo, or all"
    :default "all"
    :validate [#{ "fifo" "fanout" "topo" "all" } "must be fifo, fanout, topo, or all"]]
   [nil "--time-limit SECONDS" "Time limit for the main phase of each test"
    :default 60
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   [nil "--interval SECONDS" "Seconds between nemesis operations"
    :default 10
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   [nil "--nemesis KIND" "Fault injection: none, parts, kill, chaos"
    :default "parts"
    :validate [#{"none" "parts" "kill" "chaos"} "must be none, parts, kill, or chaos"]]
   [nil "--publish-rate RATE" "Publishes per second (per test total)"
    :default nil
    :parse-fn parse-double]
   [nil "--rate RATE" "Ops per second (topo test)"
    :default nil
    :parse-fn parse-double]
   [nil "--fanout-queues K" "Number of bound queues in the fanout test"
    :default 3
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   [nil "--fanout-publishers P" "Number of publishers in the fanout test"
    :default 2
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   [nil "--drain-limit SECONDS" "Max seconds to spend draining queues"
    :default 45
    :parse-fn parse-long]
   [nil "--username USER" "AMQP username" :default "guest"]
   [nil "--password PASS" "AMQP password" :default "guest"]
   [nil "--vhost VHOST" "AMQP vhost" :default "/"]
   [nil "--ssh-key PATH" "Path to the SSH private key for node control"
    :default "/root/.ssh/id_ed25519"]
   ["-h" "--help" "Print usage"]])

(defn- usage
  [opts-summary]
  (str "Jepsen tests for Switchboard, the multi-master AMQP 0-9-1 broker.\n\n"
       "Tests:\n"
       "  fifo    per-queue FIFO order, no loss, redelivery discipline\n"
       "  fanout  total-order broadcast across queues bound to a fanout\n"
       "  topo    linearizability of queue existence (meta raft group)\n\n"
       opts-summary))

(defn- ->opts
  [options]
  {:nodes           (vec (remove str/blank? (map str/trim (str/split (or (:nodes options) "") #","))))
   :time-limit      (:time-limit options)
   :interval        (:interval options)
   :nemesis         (keyword (:nemesis options))
   :publish-rate    (:publish-rate options)
   :rate            (:rate options)
   :fanout-queues   (:fanout-queues options)
   :fanout-publishers (:fanout-publishers options)
   :drain-limit     (:drain-limit options)
   :username        (:username options)
   :password        (:password options)
   :vhost           (:vhost options)
   :ssh-key         (:ssh-key options)})

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-opts)
        _      (when (seq errors)
                 (println "Errors:" (str/join "; " errors))
                 (System/exit 1))
        _      (when (:help options)
                 (println (usage summary))
                 (System/exit 0))
        opts   (->opts options)
        tests  (case (:test options)
                 "fifo"   [(tests/fifo opts)]
                 "fanout" [(tests/fanout opts)]
                 "topo"   [(tests/topo opts)]
                 "all"    (tests/all opts))]
    (info "Running tests:" (mapv :name tests)
          "against nodes" (:nodes opts))
    (let [results (mapv (fn [t]
                          (let [t (core/run! t)]
                            [(:name t) (:results t)]))
                        tests)]
      ;; Print a compact verdict and exit nonzero on any failure.
      (println "\n==== Jepsen results ====")
      (doseq [[name results] results]
        (println (format "%-10s overall: %s" name (:valid? results)))
        (doseq [[k v] (dissoc results :valid?)]
          (when (map? v)
            (println (format "  %-14s %s" k (:valid? v))))))
      (System/exit (if (every? (fn [[_ results]]
                                 (contains? #{true nil} (:valid? results)))
                               results)
                     0 1)))))
