#!/bin/bb
(ns templater
  (:require [babashka.fs :as fs]
            [clojure.string :as string]
            )
  (:import [java.nio.file Files
                          Path]))

;; build filename->path index

;(def source-root "./data/transcodes")
;(def target-root "./data/masterfootage")

(defn gen-random-source-and-target
  "generate test directories using one set of file names but stored in distinct path pattern"
  [source-root n-files n-depth target-root n-depth-target]
  (let [filename-gen-fn #(rand-int 1000)
        rand-path #(string/join "/" (conj
                                      (repeatedly (inc (rand-int n-depth))
                                        (fn [] (rand-int 10)))
                                      source-root))
        rand-target-path #(string/join "/" (conj
                                             (repeatedly (inc (rand-int n-depth-target))
                                               (fn [] (rand-int 10)))
                                             target-root))
        rand-fname #(str (filename-gen-fn) (apply str (take 4 (str "." (random-uuid)))))
        ;; we will see some file clashes to test catching that issue
        filenames (repeatedly n-files rand-fname)
        paths (repeatedly n-files rand-path)
        target-paths (repeatedly n-files rand-target-path)
        path-files (->> (interleave paths filenames)
                         (partition-all 2)
                         (map vec) vec)
        target-path-files (->> (interleave target-paths filenames)
                                (partition-all 2)
                                (map vec) vec)]

    [path-files
     target-path-files]))


;; make directories


(defn remove-test-trees [source-root target-root]
  (fs/delete-tree (fs/path source-root))
  (fs/delete-tree (fs/path target-root)))

(defn make-test-trees [source-root target-root]
  (->> (gen-random-source-and-target source-root 500 7 target-root 3)
    (mapv (fn [files]
            (do
              (->> files
                (mapv
                  (fn [[path file]]
                    (do (fs/create-dirs path)
                      (fs/create-file (fs/file path file)))))))))))

(comment
  (remove-test-trees)
  (make-test-trees)
  )

(defn reset-test-dir []
  (remove-test-trees)
  (make-test-trees))

(defn file-tree [dir]
  (->> (fs/glob dir "**")
    (mapv #(when (fs/regular-file? %)
             [(fs/path %) (fs/file-name %)]))
    (remove nil?)))

(comment
  (->> (file-tree source-root)
    (mapv last)
    distinct
    count)
  ;; distinct files?
  (->> (file-tree target-root)
    ;(mapv last)
    ;  distinct count
    )
  )


(defn tree-to-index [source-root file-list]
  (->> file-list
    (mapv
      #(vector
         (fs/strip-ext (last %))
         (-> (str (fs/parent (first %)))
           (string/replace
             (string/replace
               (str source-root "/") "./" "") ""))))
    (group-by first)
    (mapv (juxt first #(mapv last (last %))))
    (into {})))


(defn make-tree
  ":template option will create a directory tree from src into target, moving matching
    filenames from within target to matched newly created path. Ignores extension names
    e.g. source/1/2/myfile.txt to target/1/2/myfile.mov"
  [src-list target-list source-root target-root
   {:keys [;:copy-files
           :no-op
           :template]}]
  (let [_ (spit (str "./source-list" (inst-ms (java.util.Date.)) ".edn") (vec src-list))
        _ (spit (str "./target-list" (inst-ms (java.util.Date.)) ".edn") (vec target-list))
        source-lookup (tree-to-index source-root src-list)
        get-source-paths-fn (fn [f] (get source-lookup (fs/strip-ext f)))
        _ (def source-lookup' source-lookup)
        _ (spit (str "./source-lookup-list" (inst-ms (java.util.Date.)) ".edn") source-lookup)
        target-new-tree-list (->> source-lookup
                               (mapcat last)
                               distinct
                               (mapv #(fs/path target-root %)))
        _ (spit (str "./target-new-tree-list" (inst-ms (java.util.Date.)) ".edn") target-new-tree-list)]
    (cond
      no-op
      (do (println "Not creating just printing...")
        (->> target-new-tree-list
          (mapv
            (fn [path]
              (println path)))))
      :else
      (do (println "Creating target directories to match source")
        (->> target-new-tree-list
          (mapv
            (fn [path]
              (fs/create-dirs path)
              )))))
    (cond
      (and template no-op)
      (->> target-list
        (mapv (fn [[path filename]]
                (let [target-paths (get-source-paths-fn filename)]
                  (->> target-paths
                    (mapv #(let [target (fs/path target-root %)]
                             (if (fs/exists? path)
                               (println "NO OP...move from: " path
                                 "to: " target)
                               (do (fs/create-dirs "./data/duplicate-files")
                                 (spit (fs/file "./data/duplicate-files"
                                         (str "no-op-duplicates-list-"
                                           (inst-ms (java.util.Date.))
                                           ".txt"))
                                   (str path "\n")
                                   :append true)))
                             )))))))
      template
      (->> target-list
        (mapv (fn [[path filename]]
                (let [target-paths (get-source-paths-fn filename)]
                  (->> target-paths
                    (mapv #(let [target (fs/path target-root %)]
                             (println "move from: " path
                                 "to: " target)
                             (if (fs/exists? path)
                               (fs/move path target)
                               (do (fs/create-dirs "./data/duplicate-files")
                                 (spit (fs/file
                                         (fs/file "./data/duplicate-files"
                                           (str "duplicates-list-"
                                             (inst-ms (java.util.Date.))
                                             ".txt")))
                                   (str path "\n")
                                   :append true)))))))))))))

(defn main [source-root target-root {:keys [:no-op :template]}]
  (make-tree
    (file-tree source-root)
    (file-tree target-root)
    source-root
    target-root
    {:no-op no-op :template template}))

(comment
  (main "./data/x" "./data/y" {:no-op true :template true})
  (tree-to-index (file-tree source-root))
  (file-tree target-root)

   (let [f "481"]
     (get source-lookup' (fs/strip-ext f)))

  (->> source-lookup'
    (mapv second)
    (mapv count)
    (filter #(> % 1))
    ;count
    (reduce + ))


  (->> (make-tree
         (file-tree source-root)
         (file-tree target-root)
         target-root
         {:no-op true :template true})
    ;distinct count
    )



  ;;;;;;;
  ;; Really moves files
  ;;;;;;
  (reset-test-dir)
  (->> (make-tree
         (file-tree source-root)
         (file-tree target-root)
         target-root
         {:template true})
    ;distinct count
    )

  )

(comment
  ;; need all the paths,
  ;; [path, filename] -> {filename [path, path2]}
  (def test-src-list (file-tree source-root))
  (->> test-src-list

    (group-by first)
    ;distinct
    ;count
    )
  (invert-map-of-sets test-src-list)





  (->> (-> target-root file-tree-map)
    (remove nil?)
    count))



#_ (let [from (fs/real-path source-root {:nofollow-links true})
      tree-map []]
  (fs/walk-file-tree from
    {:pre-visit-dir  (constantly :continue)
     :visit-file     (fn [from-path _attrs]
                       (let [rel (fs/relativize from from-path)]
                         (conj tree-map rel)
                         :continue))
     :post-visit-dir (constantly :continue)}))


(comment
  (gen-random-source source-root 30 6 3)
  (gen-random-source source-root 30 3)
  )