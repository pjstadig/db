;;;; Copyright © Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.db
  (:refer-clojure :exclude [keys])
  (:require [clojure.set :as set]
            [pjstadig.db.relations :as r]))

(defn relation
  [records]
  (set records))

(defn keys
  [rel]
  (r/keys rel))

(defn rename-keys
  [rel kmap]
  (r/rename-keys rel kmap))

(defn restrict
  [rel query]
  (r/restrict rel query))

(defn project
  [rel ks]
  (r/project rel ks))

(defn product
  [rel0 rel1]
  (r/product rel0 rel1))

(defn union
  [rel0 rel1]
  (r/union rel0 rel1))

(defn intersection
  [rel0 rel1]
  (r/intersection rel0 rel1))

(defn difference
  [rel0 rel1]
  (r/difference rel0 rel1))

(defn join
  ([rel0 rel1]
     (let [rel0-keys (set (keys (first rel0)))
           rel1-keys (set (keys (first rel1)))
           keys (set/intersection rel0-keys rel1-keys)]
       (if (seq keys)
         (join rel0 rel1 (into {} (for [k keys] [k k])))
         (empty rel0))))
  ([rel0 rel1 ks]
     (r/join rel0 rel1 ks)))
