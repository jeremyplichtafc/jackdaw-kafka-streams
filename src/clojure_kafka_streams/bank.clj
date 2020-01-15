(ns clojure-kafka-streams.bank
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.json :as json]
            [jackdaw.serdes :refer [string-serde]]
            [jackdaw.admin :as ja]
            [jackdaw.client :as jc]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clj-time.spec :as timespec]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [jackdaw.streams :as js])
  (:import (org.apache.kafka.common.serialization Serdes)
           (org.apache.kafka.streams StreamsConfig))
  (:gen-class))

(def customers #{"John" "Jeremy" "Kate" "Janis" "Joey" "Andy"})

(s/def ::name customers)
(s/def ::amount (s/int-in 1 1000))
(s/def ::timestamp ::timespec/date-time)
(s/def ::transaction (s/keys :req-un [::name ::amount ::timestamp]))

(def num-customers (count customers))
(defn abs [x] (max x (- x)))

(defn sleep-time
  "Takes a first-time and last-time assumed to be in ms, and returns
  the difference between the two if they are both set and that difference
  is between 0 and 1000"
  [first-time last-time]
  (or (if-let [time (and
                      first-time
                      last-time
                      (abs (- last-time first-time)))]
        (if (< time 1000)
          (- 1000 time)
          0))

      0))

(defn producer-loop [shutdown-promise num-per-second producer topic]
  (thread

    (loop [first-time (System/currentTimeMillis)
           last-time nil]

      ; calculate if our last invocation has been less than a
      ; second and if so, sleep the remaining tim
      (let [wait-time (sleep-time first-time last-time)
            promise-val (deref shutdown-promise wait-time ::timeout)]
        (if (= promise-val ::timeout)
          (do
            (doseq [x (range 0 num-per-second)]
              ; generate a val
              (jc/produce! producer
                           topic
                           nil
                           (update (gen/generate (s/gen ::transaction))
                                   :timestamp str)))
            (recur
              (System/currentTimeMillis)
              first-time)))))))


(def kafka-config
  {"application.id"                            "streaming-bank-calculator"
   "bootstrap.servers"                         "localhost:9092"
   "default.key.serde"                         (class (Serdes/String))
   "default.value.serde"                       (class (Serdes/String))
   (StreamsConfig/PROCESSING_GUARANTEE_CONFIG) (StreamsConfig/EXACTLY_ONCE)
   "cache.max.bytes.buffering"                 "0"})


(defn max-string [s1 s2]
  (if (neg? (compare s1 s2))
    s2
    s1))

;; Each topic needs a config. The important part to note is the :topic-name key.
(def transaction-input
  {:topic-name         "transaction-input"
   :partition-count    1
   :replication-factor 1
   :topic-config       {}
   :key-serde          (string-serde)
   :value-serde        (json/serde)})

(def transaction-output
  {:topic-name         "transaction-output"
   :partition-count    1
   :replication-factor 1
   :topic-config       {"cleanup.policy" "compact"}
   :key-serde          (string-serde)
   :value-serde        (json/serde)})

(defn create-bank-balance-topology [builder topic]
  (-> (js/kstream builder topic)
      (js/select-key (fn [[k v]] (:name v)))
      (js/group-by-key)
      (js/aggregate
        (fn [] {:balance 0 :lasttime ""})
        (fn [acc [k v]]
          {:balance   (+ (:amount v)
                         (:balance acc))
           :lasttime (max-string (:timestamp v)
                                 (:lasttime acc))
           :name k})
        {:topic-name "transaction-output-AGGREGATOR-STORE"
         :value-serde (json/serde)})
      (js/to-kstream)
      (js/to transaction-output)))

(def producer (delay (jc/producer
                       kafka-config
                       {:key-serde   (string-serde)
                        :value-serde (json/serde)})))
(defn start! []
  (let [builder (js/streams-builder)]
    (create-bank-balance-topology builder transaction-input)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop! [kafka-streams-app]
  (js/close kafka-streams-app)
  (.close @producer))

(def admin-client (delay (ja/->AdminClient kafka-config)))

(defn -main
  "Run some kafka streams with plain old kafka library and clojure/java interop"
  [& args]
  (let [kafka-streams-app (start!)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(stop! kafka-streams-app)))))

