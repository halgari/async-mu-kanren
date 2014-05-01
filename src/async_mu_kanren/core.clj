(ns async-mu-kanren.core
  (:refer-clojure :exclude [conj disj merge ==])
  (:require [clojure.core.async :refer [<! >! chan close! <!! >!! alts!!] :as async]
            [clojure.core.async.impl.protocols :as impl]))

(defmacro go [& body]
  `(async/go (try
               ~@body
               (catch Throwable ex#
                 (clojure.stacktrace/print-stack-trace ex#)
                 (println "--")))))

(defn duplex-pipe
  "Defines two connected duplex channels"
  []
  (let [c-> (chan 1)
        c<- (chan 1)]

    [(reify
       impl/WritePort
       (put! [this val handler]
         (impl/put! c-> val handler))
       impl/ReadPort
       (take! [this handler]
         (impl/take! c<- handler))
       impl/Channel
       (close! [this]
         (impl/close! c->))
       (closed? [this]
         (impl/closed? c->)))

     (reify
       impl/WritePort
       (put! [this val handler]
         (impl/put! c<- val handler))
       impl/ReadPort
       (take! [this handler]
         (impl/take! c-> handler))
       impl/Channel
       (close! [this]
         (impl/close! c<-))
       (closed? [this]
         (impl/closed? c<-)))]))


(def empty-state {})

(defrecord LVar [name])

(defn lvar
  ([]
   (lvar (gensym "lvar_")))
  ([name]
   (->LVar name)))

(defn lvar? [x]
  (instance? LVar x))


(defn walk [u s]
  (if-let [pr (and (lvar? u)
                (get s u nil))]
    (recur pr s)
    u))

(defn ext-s [x v s]
  (assoc s x v))

(defn == [u v]
  ())

(defn unify [u v s]
  (let [u (walk u s)
        v (walk v s)]
    (cond
      (and (lvar? u)
           (lvar? v)
           (= u v)) s

      (lvar? u) (ext-s u v s)

      (lvar? v) (ext-s v u s)

      (and (seq? u)
           (seq? v)) (let [s (unify (first u) (first v) s)]
                       (and s (unify (next u) (next v) s)))

      :else (and (= u v) s))))

(unify (lvar 4) 4 {})

(defn conj [g1 g2]
  (let [[s ret] (duplex-pipe)]
    (go (loop []
          (let [[v ch] (alts! [s g1 g2])]
            (condp identical? ch
              s (if (nil? v)
                  (do (close! g1)
                      (close! g2)
                      (close! s))
                  (do (>! g1 v)
                      (recur)))
              g1 (if (nil? v)
                   (do (close! g2)
                       (close! s))
                   (do (>! g2 v)
                       (recur)))
              g2 (if (nil? v)
                   (close! ret)
                   (do (>! s v)
                       (recur)))
              (assert false (pr-str ch))))))
    ret))

(defn disj [g1 g2]
  (let [[s ret] (duplex-pipe)
        m (async/mult s)]
    (async/tap m g1)
    (async/tap m g2)
    (async/pipe (async/merge [g1 g2]) s)
    ret))

(defn == [a b]
  (let [[s ret] (duplex-pipe)]
    (go (loop []
          (let [v (<! s)]
            (if (nil? v)
              (close! s)
              (do (>! s (unify a b v))
                  (recur))))))
    ret))

(defn test-x [x]
  (let [a (== (lvar 'f) x)
        b (== (lvar 's) (lvar 'f))
        u (conj a b)]
    u))

(let [a (test-x 42)
      b (test-x 43)
      u (disj a b)]
  (>!! u empty-state)
  (prn (alts!! [u (async/timeout 1000)]
               ))
  (prn (alts!! [u (async/timeout 1000)]
               )))