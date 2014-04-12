;;;; Copyright © Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.db.relations
  (:refer-clojure :exclude [keys])
  (:require [clojure.core :as clj]
            [clojure.set :as set]))

(defprotocol IRelation
  (keys [this])
  (rename-keys [this kmap])
  (restrict [this query])
  (project [this ks])
  (product [this rel])
  (union [this rel])
  (intersection [this rel])
  (difference [this rel])
  (join [this rel ks]))

(defn vals-fn [ks]
  (apply juxt (map (fn [k] #(get % k)) ks)))

(defn assert-disjoint-keys [ks rel]
  (when-let [i (not-empty (set/intersection (set ks) (set (keys rel))))]
    (throw (IllegalStateException. (str "ambiguous key(s) " (pr-str i))))))

(extend-protocol IRelation
  clojure.lang.PersistentHashSet
  (keys [this]
    (set (mapcat clj/keys this)))
  (rename-keys [this kmap]
    (set (for [x this] (set/rename-keys x kmap))))
  (restrict [this query]
    (set (filter query this)))
  (project [this ks]
    (set (for [x this] (select-keys x ks))))
  (product [this rel]
    (assert-disjoint-keys (keys this) rel)
    (set (for [x this y rel] (merge y x))))
  (union [this rel]
    (into this rel))
  (intersection [this rel]
    (reduce (fn [new-rel x]
              (if (contains? this x)
                (conj new-rel x)
                new-rel))
            #{}
            rel))
  (difference [this rel]
    (reduce disj this rel))
  (join [this rel kmap]
    (assert-disjoint-keys (set/difference (keys this)
                                          (set (flatten (seq kmap))))
                          rel)
    (let [idx (group-by (vals-fn (vals kmap)) rel)]
      (set (for [x this
                 :let [vs ((vals-fn (clj/keys kmap)) x)]
                 :when (seq vs)
                 :let [ys (get idx vs)]
                 :when (seq ys)
                 y ys]
             (merge y x))))))
