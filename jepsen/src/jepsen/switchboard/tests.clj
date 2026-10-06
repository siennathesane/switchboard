(ns jepsen.switchboard.tests
  "The three switchboard workloads.

  Roles are pinned to threads: with :concurrency equal to the node count,
  worker thread t drives node (nth nodes t), so thread sets and node roles
  line up. Thread predicates below rely on that mapping.

  fifo   - N-1 publishers on N-1 nodes publish confirmed, per-channel
           sequenced messages to one durable queue; one poller on the last
           node drains it with basic.get+ack. Checks per-publisher FIFO,
           no loss of confirmed messages, and redelivery discipline.

  fanout - 2 publishers publish confirmed messages to a fanout exchange
           bound to 3 durable queues, each drained by its own poller on a
           dedicated node. Checks that all queues observe the SAME order of
           confirmed messages (total-order broadcast) plus the fifo
           properties per queue.

  topo   - every worker performs queue.declare / queue.delete / passive
           declares on one register queue, spread across all nodes, with a
           full partition/kill nemesis. Checks linearizability of queue
           existence with Knossos."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [assert+]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as nemesis]
                    [net :as net]
                    [os :as os]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.sshj :as sshj]
            [jepsen.nemesis.combined :as combined]
            [jepsen.switchboard [client :as sclient]
                                [db :as sdb]
                                [checkers :as checkers]]))

(def fifo-queue "je.fifo")
(def fanout-exchange "je.fx")
(def topo-queue "je.reg")

(defn fanout-queues
  [opts]
  (mapv #(str "je.f" %) (range (:fanout-queues opts 3))))

;; Nemesis

(defn nemesis-spec
  "Returns {:nemesis :generator :final-generator :perf} for the requested
  fault kind (:none, :parts, :kill, :chaos).

  We compose packages ourselves instead of using nemesis-package: its
  default compose includes the clock/packet/file-corruption nemeses whose
  setup/teardown fetch extra tooling (bitflip, libfaketime) that we neither
  want nor need."
  [opts]
  (let [kind (or (:nemesis opts) :parts)]
    (if (= kind :none)
      {:nemesis         nemesis/noop
       :generator       nil
       :final-generator nil
       :perf            #{}}
      (let [base {:db        (sdb/db)
                  :interval  (:interval opts 10)
                  :partition {:targets [:one :majority :majorities-ring]}
                  :kill      {:targets [:one]}
                  :pause     {:targets [:one]}}
            pkgs (case kind
                   :parts [(combined/partition-package
                             (assoc base :faults #{:partition}))]
                   :kill  [(combined/db-package
                             (assoc base :faults #{:kill :pause}))]
                   :chaos [(combined/partition-package
                             (assoc base :faults #{:partition}))
                           (combined/db-package
                             (assoc base :faults #{:kill :pause}))])
            pkg  (combined/compose-packages pkgs)]
        {:nemesis         (:nemesis pkg)
         :generator       (:generator pkg)
         :final-generator (:final-generator pkg)
         :perf            (:perf pkg)}))))

;; Test scaffolding

(defn- base-test
  [name opts]
  {:name         name
   :os           os/noop
   :net          net/iptables
   :db           (sdb/db)
   :remote       (sshj/remote)
   :nodes        (:nodes opts)
   :concurrency  (:concurrency opts (count (:nodes opts)))
   :ssh          {:username               "root"
                  :private-key-path       (:ssh-key opts "/root/.ssh/id_ed25519")
                  :strict-host-key-checking false}
   :leave-db-running? false})

(defn- check-concurrency
  "Role pinning assumes one thread per node, in node order."
  [opts]
  (assert+ (= (:concurrency opts (count (:nodes opts)))
              (count (:nodes opts)))
           {:message ":concurrency must equal the node count for role pinning"}))

(defn- nemesis-checker
  [opts nem]
  (if (seq (:perf nem))
    {:perf (checker/perf (:perf nem))}
    {}))

(defn- drain-generators
  "Phases after the main load: heal the network, wait, drain via gets until
  each poller's first empty get (bounded by :drain-limit), then one :depth
  op per poller. `poll?` pins drain work to poller threads; `empty-get?`
  detects a completed drain. each-thread gives every poller its own
  until/stagger so one queue emptying doesn't stop the others."
  [opts nem poll? empty-get?]
  [(gen/log "Healing the network")
   (when (:final-generator nem) (gen/nemesis (:final-generator nem)))
   (gen/sleep (:settle opts 3))
   (gen/clients
     (gen/on-threads poll?
       (gen/time-limit (:drain-limit opts 45)
         (gen/each-thread
           (gen/until empty-get?
             (gen/stagger (/ 1 40) (repeat {:type :invoke, :f :get})))))))
   (gen/clients
     (gen/on-threads poll?
       (gen/each-thread (gen/once {:type :invoke, :f :depth}))))])

(defn- empty-get?
  "Pred for gen/until: a successful get that found the queue empty."
  [op]
  (and (h/ok? op)
       (= :get (:f op))
       (nil? (:msg (:value op)))))

;; Workloads

(defn fifo
  "Single-queue FIFO test. Publishers on nodes 0..n-2; the poller lives on
  the last node."
  [opts]
  (let [nodes  (:nodes opts)
        n      (count nodes)
        _      (check-concurrency opts)
        poller-thread (dec n)
        pub?   #(not= % poller-thread)
        poll?  #(= % poller-thread)
        nem    (nemesis-spec opts)
        pub-rate (:publish-rate opts 10)]
    (assoc (base-test "sb-fifo" opts)
           :client (sclient/client
                     {:conn-opts      (select-keys opts [:username :password :vhost])
                      ;; only the poller node reads; publishers pass nil
                      :queues-by-node (zipmap nodes
                                              (concat (repeat (dec n) nil)
                                                      [fifo-queue]))
                      :publish        {:exchange "", :routing-key fifo-queue}
                      :topology       {:queues [fifo-queue]}})
           :nemesis (:nemesis nem)
           :generator (apply gen/phases
                             (->> (gen/any
                                    (gen/on-threads pub?
                                      (gen/stagger (/ 1 pub-rate)
                                        (repeat {:type :invoke, :f :publish})))
                                    (gen/on-threads poll?
                                      (gen/stagger 1/25
                                        (repeat {:type :invoke, :f :get}))))
                                  (gen/nemesis (:generator nem))
                                  (gen/time-limit (:time-limit opts 60)))
                             (drain-generators opts nem poll? empty-get?))
           :checker (checker/compose
                      (merge {:fifo     (checkers/fifo-checker)
                              :stats    (checker/stats)
                              :timeline (timeline/html)}
                             (nemesis-checker opts nem))))))

(defn fanout
  "Total-order-broadcast test over a fanout exchange. The first
  :fanout-publishers nodes publish; the remaining nodes each poll one bound
  queue."
  [opts]
  (let [nodes  (:nodes opts)
        n      (count nodes)
        _      (check-concurrency opts)
        k      (:fanout-queues opts 3)
        pubs   (:fanout-publishers opts 2)
        queues (fanout-queues opts)
        _      (assert+ (= n (+ pubs k))
                        {:message (str "fanout needs node count = publishers("
                                       pubs ") + queues(" k ")")})
        pub?   (set (range pubs))
        poll?  (set (range pubs n))
        nem    (nemesis-spec opts)
        pub-rate (:publish-rate opts 8)]
    (assoc (base-test "sb-fanout" opts)
           :client (sclient/client
                     {:conn-opts      (select-keys opts [:username :password :vhost])
                      :queues-by-node (zipmap nodes
                                              (concat (repeat pubs nil) queues))
                      :publish        {:exchange fanout-exchange, :routing-key ""}
                      :topology       {:queues queues
                                       :exchange fanout-exchange}})
           :nemesis (:nemesis nem)
           :generator (apply gen/phases
                             (->> (gen/any
                                    (gen/on-threads pub?
                                      (gen/stagger (/ 1 pub-rate)
                                        (repeat {:type :invoke, :f :publish})))
                                    (gen/on-threads poll?
                                      (gen/stagger 1/25
                                        (repeat {:type :invoke, :f :get}))))
                                  (gen/nemesis (:generator nem))
                                  (gen/time-limit (:time-limit opts 60)))
                             (drain-generators opts nem poll? empty-get?))
           :checker (checker/compose
                      (merge {:fanout   (checkers/fanout-checker)
                              :stats    (checker/stats)
                              :timeline (timeline/html)}
                             (nemesis-checker opts nem))))))

(defn topo
  "Linearizability of the queue-existence register."
  [opts]
  (let [nodes (:nodes opts)
        nem   (nemesis-spec opts)
        rate  (:rate opts 5)
        dcl   {:type :invoke, :f :declare}
        del   {:type :invoke, :f :delete}
        exs   {:type :invoke, :f :exists}]
    (assoc (base-test "sb-topo" opts)
           :client (sclient/client
                     {:conn-opts (select-keys opts [:username :password :vhost])
                      :register  topo-queue})
           :nemesis (:nemesis nem)
           :generator (gen/phases
                        (->> (gen/mix (mapv gen/repeat [dcl dcl exs exs del]))
                             (gen/stagger (/ 1 rate))
                             (gen/nemesis (:generator nem))
                             (gen/time-limit (:time-limit opts 60)))
                        (gen/log "Healing the network")
                        (when (:final-generator nem)
                          (gen/nemesis (:final-generator nem))))
           :checker (checker/compose
                      (merge {:linearizable (checker/linearizable
                                              {:model     (checkers/ex-reg)
                                               :algorithm :wgl})
                              :stats        (checker/stats)
                              :timeline     (timeline/html)}
                             (nemesis-checker opts nem))))))

(defn all
  [opts]
  [(fifo opts) (fanout opts) (topo opts)])
