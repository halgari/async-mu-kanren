(ns async-mu-kanren.core
  (:refer-clojure :exclude [conj disj merge ==])
  (:require [clojure.core.async :refer [<! >! chan close! <!! >!! alts!!] :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.core.logic :as l]
            [clojure.core.logic.protocols :as lp]))

;; An implementation of muKanren (http://webyrd.net/scheme-2013/papers/HemannMuKanren2013.pdf)
;; using core.async channels (CSP) instead of lazy streams and using CSP processes instead
;; of monads

;; Helper to print out stacktraces when gos fail
(defmacro go [& body]
  `(async/go (try
               ~@body
               (catch Throwable ex#
                 (clojure.stacktrace/print-stack-trace ex#)
                 (println "--")))))

;; Goals will return a single channel. Items put into this channel will be processed by the goal
;; and then the result can be taken from this same channel. Notice this is completely different
;; from standard CSP channels, which only allow simplex communication. We're going to leverage
;; core.async's interfaces to create a duplex channel.

;; This function will return two channel like things items put into the first channel can be read
;; from the second and vice-versa.

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

;; Logging helpers, call log to print data to the console without the output
;; getting clobbered by other processes
(let [c (chan)]
  (go (loop []
        (println (<! c))
        (recur)))
  (defn log [& args]
    (async/>!! c (vec args))))

;; muKanren defines conj as a goal that takes two goals, passing results to the second
;; only if the fist passes. If we assume goals will not return a value if the goal fails,
;; then we can easily define conj in terms of pipe.
(defn conj
  ([g1] g1)
  ([g1 g2]
   (let [[s ret] (duplex-pipe)]
     (async/pipe s g1)
     (async/pipe g1 g2)
     (async/pipe g2 s)
     ret))
  ([g1 g2 & more]
   (conj g1 (apply conj g1 more))))

;; disj is defined as a parallel trying of two goals. Core.async supplies broadcast facilities
;; in the form of `mux` we will leverage these and pipe the resuls form the gos to the output
(defn disj [& goals]
  (let [[s ret] (duplex-pipe)
        m (async/mult s)]
    (doseq [g goals]
      (async/tap m g)
      (async/pipe g s))
    ret))

;; unification goal. Originally this code used a custom unifier as specified in the paper above,
;; instead we now leverage the unification engine of Core.Logic. This this allows us to take advantage
;; of things like LCons with minimal effort.

;; the unification gal can be define din therms of pipe map and removal of nils.
(defn == [a b]
  (let [[s ret] (duplex-pipe)]
    (async/pipe
      s
      (async/map> (fn [s]
                    #_(log [s a b])
                    (l/unify s a b))
            (async/remove> nil? s)))
    ret))

;; fresh defines lvars in an outer let and then simply applies conj to all the goals in the body
(defmacro fresh [lvars & goals]
  `(let [~@(vec (mapcat (fn [var]
                         `[~var (l/lvar ~(name var))])
                       lvars))]
    ~(if (> (count goals) 1)
       `(conj ~@goals)
       (first goals))))

;; conde is a disj wrapping bodies wrapped in conj
(defn conde [& goals]
  (apply disj (map (partial apply conj) goals)))

;; given a set of lvars, pull walk them from the substitution map and return a seq of the resulting values.
(defn -run-chan [lvars g]
  (let [[s ret] (duplex-pipe)]
    (async/pipe s g)
    (async/pipe (async/map< (fn [s]
                              (map #(lp/walk s %) lvars))
                            g)
                s)
    ret))

;; like core.logic's run-lazy but returns a channel
(defmacro run-chan [lvars & goals]
  `(let [lvars# ~(vec (map (fn [var]
                             `(l/lvar (gensym ~(name var))))
                           lvars))
         ~lvars lvars#
         r# ~(if (> (count goals) 1)
               `(conj ~@goals)
               (first goals))]
     (-run-chan lvars# r#)))

(defn close-after [msec c]
  (go (<! (async/timeout msec))
      (close! c))
  c)

;; Runs until n items have been found, or returns
(defmacro run [n timeout args & body]
  `(let [r# (run-chan ~args ~@body)
         t# (async/timeout ~timeout)]
     (>!! r# l/empty-s)
     (<!! (async/into [] (async/take ~n (close-after ~timeout r#))))))


(async/alt!! (async/timeout 100) ([_] _))

(defn emptyo
  [a]
  (== '() a))

(defn conso
  [a d l]
  (== (l/lcons a d) l))

(defn firsto
  [l a]
  (fresh [d]
         (conso a d l)))

(defn resto
  [l d]
  (fresh [a]
         (conso a d l)))


;; Helper for delay goal
(defn sink [c f]
  (go (loop []
        (let [v# (<! c)]
          (when (not (nil? v#))
            (f v#)
            (recur))))))

;; Unlike monad based kanren implementations, this CSP implementation eagerly creates graphs
;; before execution. This can cause a stack overflow on recursive goals. This macro makes
;; a goal lazy, that is, it's CSP dataflow will not be created until needed. And even then
;; it will be destroyed and recreated for each substitution.

;; TODO - Figure out if it is possible to have a goal be recursive by having its body feed
;; back into its input

(defmacro delay-goal [goal]
  `(let [[s# ret#] (duplex-pipe)
         make-goal# (fn [] ~goal)]
     (sink s# (fn [sub#]
                (let [goal# (make-goal#)]
                  (async/put! goal# sub#)
                  (async/pipe goal# s#))))
     ret#))

;; Define membero, and don't forget to delay the recursive call.
(defn membero [x col]
  (conde
    [(firsto col x)]
    [(fresh [tail]
            (resto col tail)
            (delay-goal (membero x tail)))]))

;; Some simple examples

(run 2 1000 [q]
     (conde
       [(== q 1)]
       [(== q 2)]))

(run 4 1000 [q]
     (membero q '(1 2 3 4)))

;; Only one answer, so times out after 1000 ms

(run 2 1000 [q]
     (membero q '(3 4))
     (membero q '(1 2 3 4))
     (membero q '(4)))


;; Async goals are possible

;; Put items onto a channel, but wait ms milliseconds inbetween each item.
(defn onto-chan-limit [c ms coll]
  (go (doseq [x coll]
        (<! (async/timeout ms))
        (>! c x))))

;; Non relational. Given a lvar 'max' unify with 0-max, but do it slowly,
;; one unification a second.
(defn upto [max out]
  (let [[s ret] (duplex-pipe)]
    (sink s (fn [a]
              (let [max (lp/walk a max)]
                (assert (not (l/lvar? max)) "max must be bound")
                (->> (range max)
                     (map (partial l/unify a out))
                     (onto-chan-limit s 1000)))))
    ret))


;; Notice how the results slowly arrive, even though the CPU is idle
(let [rc (run-chan [q]
                   (upto 100 q))]
  (>!! rc l/empty-s)
  (dotimes [x 10]
    (println (<!! rc))))

