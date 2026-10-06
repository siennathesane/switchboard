(ns jepsen.switchboard.amqp
  "Thin wrapper over the RabbitMQ Java client (AMQP 0-9-1), used by the
  Jepsen clients. We deliberately keep automatic recovery OFF: Jepsen wants
  connection failures to surface in the history, not be papered over."
  (:require [clojure [string :as str]])
  (:import (com.rabbitmq.client ConnectionFactory Connection Channel
                                ConfirmListener GetResponse
                                ShutdownSignalException
                                MessageProperties)
           (java.nio.charset StandardCharsets)))

(def default-port 5672)

(defn factory
  "A ConnectionFactory for a switchboard node. Automatic recovery is
  disabled: the Jepsen client manages reconnections itself so that faults
  are visible as operation outcomes."
  ^ConnectionFactory [host port user pass vhost]
  (doto (ConnectionFactory.)
    (.setHost host)
    (.setPort (int port))
    (.setUsername user)
    (.setPassword pass)
    (.setVirtualHost (or vhost "/"))
    (.setRequestedHeartbeat 10)
    (.setConnectionTimeout 5000)
    (.setAutomaticRecoveryEnabled false)
    (.setTopologyRecoveryEnabled false)))

(defn connect
  "Opens a connection to `node`; returns a Connection."
  [node {:keys [port username password vhost] :or {port default-port
                                                   username "guest"
                                                   password "guest"
                                                   vhost "/"}}]
  (.newConnection (factory node port username password vhost)))

(defn close-quietly
  "Closes a connection or channel, swallowing errors."
  [x]
  (when x
    (try (.close x) (catch Exception _))))

(defn open?
  "Is this connection/channel still usable?"
  [x]
  (and x (.isOpen x)))

(defn channel
  "Opens a fresh channel on `conn`."
  ^Channel [^Connection conn]
  (.createChannel conn))

(defn error-info
  "Walks an exception chain looking for a ShutdownSignalException; returns
  {:code n :text s} for AMQP channel/connection errors, or {:code nil ...}
  for other failures."
  [^Exception e]
  (loop [t e]
    (when t
      (cond
        (instance? ShutdownSignalException t)
        (let [reason (.getReason ^ShutdownSignalException t)]
          (try
            {:code  (.getReplyCode reason)
             :text  (.getReplyText reason)
             :hard? (.isHardError ^ShutdownSignalException t)}
            (catch Exception _
              {:code nil, :text (.getMessage ^ShutdownSignalException t)})))

        ;; Descend the cause chain; if there is none, report flat.
        (.getCause t) (recur (.getCause t))

        :else {:code nil, :text (str (.getName (class t)) ": " (.getMessage t))}))))

(defn error-code
  "The AMQP reply code from an exception, or nil."
  [e]
  (:code (error-info e)))

(defn resolve-pending!
  "Delivers `result` to the promise at confirm seq `tag` in the `pending`
  atom (seq -> promise), plus (if `multiple`) every pending tag below it."
  [pending tag result multiple]
  (doseq [t (if multiple
              (->> @pending keys (filter #(<= (long ^long %) (long tag))))
              [tag])]
    (when-let [p (@pending t)]
      (swap! pending dissoc t)
      (deliver p result))))

(defn fail-pending!
  "Resolves every outstanding confirm promise with `result` (used when a
  channel dies: outstanding publishes become indeterminate)."
  [pending result]
  (doseq [t (keys @pending)]
    (when-let [p (@pending t)]
      (swap! pending dissoc t)
      (deliver p result))))

(defn confirm-channel!
  "Returns a channel with confirm mode selected and `pending` (an atom of
  confirm seq -> promise) wired to its ConfirmListener. Acks resolve their
  promise with :ack; nacks with :nack. With `multiple`, every outstanding
  seq <= tag resolves."
  [^Connection conn pending]
  (let [ch (channel conn)
        l  (reify ConfirmListener
             (handleAck [_ tag multiple]
               (resolve-pending! pending tag :ack multiple))
             (handleNack [_ tag multiple]
               (resolve-pending! pending tag :nack multiple)))]
    (.confirmSelect ch)
    (.addConfirmListener ch ^ConfirmListener l)
    ch))

(defn- body->bytes
  [body]
  (if (instance? String body)
    (.getBytes ^String body StandardCharsets/UTF_8)
    body))

(defn publish!
  "Publishes `body` (String or byte[]) to `exchange` with `routing-key`. For
  queue publishes pass the default exchange (\"\") and the queue name as the
  routing key. Returns the confirm seq for this publish."
  [^Channel ch exchange routing-key body]
  (.basicPublish ch exchange routing-key false false
                 MessageProperties/MINIMAL_PERSISTENT_BASIC (body->bytes body))
  (.getNextPublishSeqNo ch))

(defn publish-confirm!
  "Publishes and awaits that seq's confirm for up to `timeout-ms`, returning
  :ack, :nack, or :timeout. Registers the promise before publishing (confirms
  arrive on the connection thread)."
  [^Channel ch pending exchange routing-key body timeout-ms]
  ;; .getNextPublishSeqNo returns the seq the NEXT publish will receive.
  (let [seq (.getNextPublishSeqNo ch)
        p   (promise)]
    (swap! pending assoc seq p)
    (.basicPublish ch exchange routing-key false false
                   MessageProperties/MINIMAL_PERSISTENT_BASIC
                   (body->bytes body))
    (deref p timeout-ms :timeout)))

(defn get-message
  "basic.get with auto-ack off. Returns nil when the queue is empty, else
  {:tag :redelivered :depth :body}."
  [^Channel ch queue]
  (when-let [^GetResponse r (.basicGet ch queue false)]
    {:tag         (.getDeliveryTag (.getEnvelope r))
     :redelivered (.isRedeliver (.getEnvelope r))
     :depth       (.getMessageCount r)
     :body        (String. (.getBody r) StandardCharsets/UTF_8)}))

(defn ack
  "basic.ack a single delivery."
  [^Channel ch tag]
  (.basicAck ch tag false))

(defn declare-queue!
  "Declares a durable, non-exclusive, non-auto-delete queue. Returns
  {:queue q :depth n}."
  [^Channel ch queue]
  (let [ok (.queueDeclare ch queue true false false nil)]
    {:queue (.getQueue ok)
     :depth (.getMessageCount ok)}))

(defn declare-queue-passive!
  "Passive declare; returns {:queue q :depth n}. Throws on a missing queue
  (404)."
  [^Channel ch queue]
  (let [ok (.queueDeclarePassive ch queue)]
    {:queue (.getQueue ok)
     :depth (.getMessageCount ok)}))

(defn delete-queue!
  "queue.delete; returns the purged message count."
  [^Channel ch queue]
  (let [ok (.queueDelete ch queue false false)]
    (.getMessageCount ok)))

(defn declare-fanout!
  "Declares a durable fanout exchange."
  [^Channel ch exchange]
  (.exchangeDeclare ch exchange "fanout" true))

(defn bind!
  "Binds a queue to an exchange."
  [^Channel ch queue exchange]
  (.queueBind ch queue exchange ""))

(defn encode-msg
  "Message body: \"node#uid:seq\"."
  [pid ^long seq]
  (str pid ":" seq))

(defn decode-msg
  "Parses \"node#uid:seq\" into [pid seq] or nil if malformed."
  [s]
  (let [[pid seq] (str/split s #":" 2)]
    (when (and pid seq)
      (try
        [pid (Long/parseLong seq)]
        (catch NumberFormatException _ nil)))))
