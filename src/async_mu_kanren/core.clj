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

(defn conj
  ([g1] g1)
  ([g1 g2]
   (let [[s ret] (duplex-pipe)]
     (async/pipe s g1)
     (async/pipe (async/remove< false? g1) g2)
     (async/pipe (async/remove< false? g2) s)
     ret))
  ([g1 g2 & more]
   (apply conj (conj g1 g2) more)))

(defn disj [& goals]
  (let [[s ret] (duplex-pipe)
        m (async/mult s)]
    (doseq [g goals]
      (async/tap m g))
    (async/pipe (async/merge (vec goals)) s)
    ret))

(defn == [a b]
  (let [[s ret] (duplex-pipe)]
    (async/pipe (async/map< #(unify a b %) s) s)
    ret))

(defn test-x [x]
  (let [a (== (lvar 'f) x)
        b (== (lvar 's) (lvar 'f))
        u (conj a b)]
    u))

(defmacro fresh [lvars & goals]
  `(let ~(vec (mapcat (fn [var]
                         `[~var (lvar (gensym ~(name var)))])
                       lvars))
     ~(if (> (count goals) 1)
        `(conj ~@goals)
        (first goals))))

(defn conde [& goals]
  (apply disj (map (partial apply conj) goals)))

(defn -run-chan [lvars g]
  (let [[s ret] (duplex-pipe)]
    (async/pipe s g)
    (async/pipe (async/map< (fn [s]
                              (map #(walk % s) lvars))
                            g)
                s)
    ret))

(defmacro run-chan [lvars & goals]
  `(let [lvars# ~(vec (map (fn [var]
                            `(lvar (gensym ~(name var))))
                          lvars))
         ~lvars lvars#
         r# ~(if (> (count goals) 1)
               `(conj ~@goals)
               (first goals))]
     (-run-chan lvars# r#)))

(macroexpand '(run-chan [q] (== q 1)))

(let [a (test-x 42)
      b (test-x 43)
      u (disj a b)
      #_u #_(fresh [x y z]
               (== z y)
               (== x 1)
               (== x y))
      u (conde
          [(fresh [x y]
                  (== x 2)
                  (== y 1))]
          [(fresh [z]
                  (== z 4))])
      u (run-chan [q v]
                  (== q 1)
                  (== v 1))]
  (>!! u empty-state)
  (prn (alts!! [u (async/timeout 1000)]
               ))
  (prn (alts!! [u (async/timeout 1000)]
               )))