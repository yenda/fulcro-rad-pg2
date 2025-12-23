(ns com.fulcrologic.rad.database-adapters.test-helpers.generators
  "Test.check generators for all supported SQL types and delta structures.

   Generators for types:
   - Strings (normal, nasty, boundary)
   - Numeric (int, long, decimal)
   - Temporal (instant, inst/epoch)
   - Boolean, Enum, Keyword, Symbol, Password
   - UUID, Tempid

   Generators for deltas:
   - Single entity inserts
   - Multi-entity inserts
   - Inserts with references
   - Updates, deletes
   - Mixed operations"
  (:require
   [clojure.test.check.generators :as gen]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid])
  (:import
   [java.math BigDecimal]
   [java.time Instant]))

;; =============================================================================
;; Primitive Generators
;; =============================================================================

(def gen-tempid
  "Generator for Fulcro tempids"
  (gen/fmap (fn [_] (tempid/tempid)) gen/nat))

(def gen-uuid
  "Generator for random UUIDs"
  (gen/fmap (fn [_] (java.util.UUID/randomUUID)) gen/nat))

;; =============================================================================
;; String Generators
;; =============================================================================

;; Problematic strings that could break SQL or encoding
(def sql-injection-strings
  ["Robert'); DROP TABLE accounts;--"
   "'; DELETE FROM accounts; --"
   "1; UPDATE accounts SET name='hacked'"
   "' OR '1'='1"
   "\" OR \"1\"=\"1"
   "1; TRUNCATE TABLE accounts; --"
   "'; EXEC xp_cmdshell('dir'); --"])

(def special-char-strings
  ["O'Brien"                          ; Single quote
   "John \"The Boss\" Smith"          ; Double quotes
   "Line1\nLine2"                     ; Newline
   "Tab\there"                        ; Tab
   "Back\\slash"                      ; Backslash
   "Semi;colon"                       ; Semicolon
   "Percent%sign"                     ; SQL LIKE wildcard
   "Under_score"                      ; SQL LIKE wildcard
   "日本語テスト"                       ; Japanese
   "Ümläüts"                          ; Umlauts
   "🎉 Emoji 🚀"                       ; Emoji
   "   leading spaces"
   "trailing spaces   "
   "  both  spaces  "
   "foo\rbar"                         ; Carriage return
   "a\u0001b"])                       ; Control character (not null)

(defn boundary-strings
  "Generate boundary strings for a given max length"
  [max-len]
  ["" ; Empty string
   "x" ; Single char
   (apply str (repeat (dec max-len) "x")) ; One under limit
   (apply str (repeat max-len "x"))]) ; At limit

(def gen-nasty-string
  "Generator for strings that might break things"
  (gen/one-of [(gen/elements sql-injection-strings)
               (gen/elements special-char-strings)
               (gen/elements (boundary-strings 200))]))

(def gen-nice-string
  "Generator for normal alphanumeric strings (1-50 chars)"
  (gen/such-that #(and (not (empty? %)) (<= (count %) 50))
                 gen/string-alphanumeric
                 100))

(def gen-string-value
  "Generator for string field values - mixes nice and nasty"
  (gen/frequency [[7 gen-nice-string]   ; 70% normal strings
                  [3 gen-nasty-string]])) ; 30% problematic strings

(defn gen-string-with-max-length
  "Generator for strings respecting a max length"
  [max-len]
  (gen/frequency
   [[7 (gen/such-that #(<= (count %) max-len)
                      gen/string-alphanumeric
                      100)]
    [3 (gen/elements (concat sql-injection-strings
                             special-char-strings
                             (boundary-strings max-len)))]]))

;; =============================================================================
;; Numeric Generators
;; =============================================================================

(def gen-int-value
  "Generator for :int type (INTEGER, 32-bit signed)"
  (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE))

(def gen-long-value
  "Generator for :long type (BIGINT, 64-bit signed)"
  gen/large-integer)

(def gen-decimal-value
  "Generator for :decimal type (decimal(20,2))
   Generates BigDecimal with 2 decimal places"
  (gen/fmap (fn [[whole frac]]
              (BigDecimal. (format "%d.%02d" whole (Math/abs (mod frac 100)))))
            (gen/tuple gen/large-integer gen/small-integer)))

(def gen-positive-int
  "Generator for positive integers"
  gen/nat)

(def gen-positive-decimal
  "Generator for positive decimals (for prices, quantities)"
  (gen/fmap (fn [[dollars cents]]
              (BigDecimal. (format "%d.%02d" (Math/abs dollars) (mod (Math/abs cents) 100))))
            (gen/tuple (gen/choose 0 10000) gen/small-integer)))

;; =============================================================================
;; Temporal Generators
;; =============================================================================

(def gen-instant-value
  "Generator for :instant type (java.time.Instant)
   Generates instants within reasonable range (2000-2030)"
  (gen/fmap (fn [epoch-seconds]
              (Instant/ofEpochSecond epoch-seconds))
            (gen/choose 946684800 1893456000))) ; 2000-01-01 to 2030-01-01

(def gen-inst-value
  "Generator for :inst type (epoch milliseconds as long)"
  (gen/fmap (fn [instant]
              (.toEpochMilli instant))
            gen-instant-value))

;; =============================================================================
;; Boolean Generator
;; =============================================================================

(def gen-boolean-value
  "Generator for boolean field values"
  gen/boolean)

;; =============================================================================
;; Enum Generator
;; =============================================================================

(defn gen-enum-value
  "Generator for enum values from a set of allowed values"
  [enum-set]
  (gen/elements (vec enum-set)))

(def gen-state-enum
  "Generator for state enum values (from test attributes)"
  (gen-enum-value #{:state/AZ :state/KS :state/MS}))

;; =============================================================================
;; Keyword and Symbol Generators
;; =============================================================================

(def gen-keyword-value
  "Generator for :keyword type (stored as VARCHAR)
   Generates namespaced and simple keywords"
  (gen/one-of
   [(gen/fmap (fn [[ns name]]
                (keyword ns name))
              (gen/tuple gen-nice-string gen-nice-string))
    (gen/fmap keyword gen-nice-string)]))

(def gen-symbol-value
  "Generator for :symbol type (stored as VARCHAR)
   Generates namespaced and simple symbols"
  (gen/one-of
   [(gen/fmap (fn [[ns name]]
                (symbol ns name))
              (gen/tuple gen-nice-string gen-nice-string))
    (gen/fmap symbol gen-nice-string)]))

;; =============================================================================
;; Password Generator
;; =============================================================================

(def gen-password-value
  "Generator for :password type (VARCHAR(512))
   Includes typical password patterns and edge cases"
  (gen/one-of
   [(gen-string-with-max-length 512)
    (gen/elements ["password123"
                   "P@ssw0rd!"
                   "🔐SecurePass123!"
                   (apply str (repeat 512 "x"))])]))

;; =============================================================================
;; Email Generator (for validation testing)
;; =============================================================================

(def gen-email-value
  "Generator for email-like strings - includes edge cases"
  (gen/frequency
   [[7 (gen/fmap (fn [[name domain]]
                   (str name "@" domain ".com"))
                 (gen/tuple gen-nice-string gen-nice-string))]
    [1 (gen/return "")]                           ; Empty email
    [1 (gen/return "not-an-email")]               ; Invalid format
    [1 (gen/return "test@evil.com'; DROP TABLE--")] ; SQL injection
    ]))

;; =============================================================================
;; Delta Structure Generators
;; =============================================================================

(defn gen-field-delta
  "Generate a field delta with :after (insert) or :before/:after (update)"
  [gen-value]
  (gen/one-of
   ;; Insert (just :after)
   [(gen/fmap (fn [v] {:after v}) gen-value)]
   ;; Update (both :before and :after)
   [(gen/fmap (fn [[before after]] {:before before :after after})
              (gen/tuple gen-value gen-value))]))

(defn gen-entity-delta
  "Generate a delta entry for an entity.
   field-gens is a map of field-key -> generator"
  [id-key field-gens]
  (gen/let [id gen-tempid
            fields (apply gen/hash-map
                          (mapcat (fn [[k gen-v]]
                                    [k (gen-field-delta gen-v)])
                                  field-gens))]
    {[id-key id] fields}))

;; =============================================================================
;; Account Delta Generators
;; =============================================================================

(def gen-account-insert
  "Generator for an account insert delta entry"
  (gen/let [tempid gen-tempid
            name gen-string-value
            include-active? gen/boolean
            active? gen-boolean-value
            include-email? gen/boolean
            email gen-email-value]
    (let [base-fields {:account/name {:after name}}
          fields (cond-> base-fields
                   include-active? (assoc :account/active? {:after active?})
                   include-email? (assoc :account/email {:after email}))]
      {[:account/id tempid] fields})))

(def gen-address-insert
  "Generator for an address insert delta entry"
  (gen/let [tempid gen-tempid
            street gen-string-value
            include-city? gen/boolean
            city gen-string-value
            include-state? gen/boolean
            state gen-state-enum]
    (let [base-fields {:address/street {:after street}}
          fields (cond-> base-fields
                   include-city? (assoc :address/city {:after city})
                   include-state? (assoc :address/state {:after state}))]
      {[:address/id tempid] fields})))

;; =============================================================================
;; Product Delta Generators (tests numeric types)
;; =============================================================================

(def gen-product-insert
  "Generator for a product insert - tests int, long, decimal, keyword, symbol"
  (gen/let [tempid gen-tempid
            name (gen-string-with-max-length 100)
            quantity gen-positive-int
            sku gen-long-value
            price gen-positive-decimal
            category gen-keyword-value
            status gen-symbol-value
            api-key gen-password-value]
    {[:product/id tempid]
     {:product/name {:after name}
      :product/quantity {:after quantity}
      :product/sku {:after sku}
      :product/price {:after price}
      :product/category {:after (str category)}  ; keywords stored as strings
      :product/status {:after (str status)}      ; symbols stored as strings
      :product/api-key {:after api-key}}}))

;; =============================================================================
;; Event Delta Generators (tests temporal types)
;; =============================================================================

(def gen-event-insert
  "Generator for an event insert - tests instant and inst types"
  (gen/let [tempid gen-tempid
            name gen-string-value
            starts-at gen-instant-value
            created-at gen-inst-value]
    {[:event/id tempid]
     {:event/name {:after name}
      :event/starts-at {:after starts-at}
      :event/created-at {:after created-at}}}))

;; =============================================================================
;; Category Delta Generators (tests self-referential)
;; =============================================================================

(def gen-category-insert
  "Generator for a category insert"
  (gen/let [tempid gen-tempid
            name gen-string-value
            position gen-positive-int]
    {[:category/id tempid]
     {:category/name {:after name}
      :category/position {:after position}}}))

(def gen-category-with-parent
  "Generator for a category with parent reference"
  (gen/let [parent-tempid gen-tempid
            child-tempid gen-tempid
            parent-name gen-string-value
            child-name gen-string-value]
    {[:category/id parent-tempid]
     {:category/name {:after parent-name}
      :category/position {:after 1}}
     [:category/id child-tempid]
     {:category/name {:after child-name}
      :category/position {:after 2}
      :category/parent {:after [:category/id parent-tempid]}}}))

;; =============================================================================
;; Composite Delta Generators
;; =============================================================================

(def gen-single-insert-delta
  "Generator for a delta with a single insert of any type"
  (gen/one-of [gen-account-insert
               gen-address-insert
               gen-product-insert
               gen-event-insert
               gen-category-insert]))

(def gen-multi-insert-delta
  "Generator for a delta with multiple inserts"
  (gen/let [num-accounts (gen/choose 1 2)
            num-addresses (gen/choose 0 2)
            account-inserts (gen/vector gen-account-insert num-accounts)
            address-inserts (gen/vector gen-address-insert num-addresses)]
    (apply merge (concat account-inserts address-inserts))))

(def gen-insert-with-reference-delta
  "Generator for inserting account with reference to new address"
  (gen/let [account-tempid gen-tempid
            address-tempid gen-tempid
            account-name gen-string-value
            street gen-string-value]
    {[:account/id account-tempid]
     {:account/name {:after account-name}
      :account/primary-address {:after [:address/id address-tempid]}}
     [:address/id address-tempid]
     {:address/street {:after street}}}))

(def gen-all-types-delta
  "Generator that creates entities exercising all types in one delta"
  (gen/let [;; Account (string, boolean, ref)
            account-tempid gen-tempid
            account-name gen-string-value
            account-active gen-boolean-value
            ;; Address (string, enum)
            address-tempid gen-tempid
            street gen-string-value
            state gen-state-enum
            ;; Product (int, long, decimal, keyword, symbol, password)
            product-tempid gen-tempid
            product-name (gen-string-with-max-length 100)
            quantity gen-positive-int
            sku gen-long-value
            price gen-positive-decimal
            category gen-keyword-value
            status gen-symbol-value
            api-key gen-password-value
            ;; Event (instant, inst)
            event-tempid gen-tempid
            event-name gen-string-value
            starts-at gen-instant-value
            created-at gen-inst-value]
    {[:account/id account-tempid]
     {:account/name {:after account-name}
      :account/active? {:after account-active}
      :account/primary-address {:after [:address/id address-tempid]}}

     [:address/id address-tempid]
     {:address/street {:after street}
      :address/state {:after state}}

     [:product/id product-tempid]
     {:product/name {:after product-name}
      :product/quantity {:after quantity}
      :product/sku {:after sku}
      :product/price {:after price}
      :product/category {:after (str category)}
      :product/status {:after (str status)}
      :product/api-key {:after api-key}}

     [:event/id event-tempid]
     {:event/name {:after event-name}
      :event/starts-at {:after starts-at}
      :event/created-at {:after created-at}}}))

;; =============================================================================
;; Chaos Mode Generators (Invalid Data)
;; =============================================================================

(def gen-null-byte-string
  "Generator for strings containing null bytes (invalid in PostgreSQL)"
  (gen/fmap (fn [[before after]]
              (str before "\u0000" after))
            (gen/tuple gen-nice-string gen-nice-string)))

(def gen-oversized-string
  "Generator for strings exceeding varchar(200) limit"
  (gen/fmap (fn [n]
              (apply str (repeat (+ 201 n) "x")))
            (gen/choose 0 100)))

(def gen-chaos-string
  "Generator that produces schema-violating strings"
  (gen/one-of [gen-null-byte-string
               gen-oversized-string]))

(def gen-chaos-delta
  "Generator for deltas containing invalid data"
  (gen/let [tempid gen-tempid
            chaos-name gen-chaos-string]
    {[:account/id tempid] {:account/name {:after chaos-name}}}))
