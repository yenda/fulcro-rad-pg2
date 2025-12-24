(ns com.fulcrologic.rad.database-adapters.test-helpers.attributes
  "Test attributes covering all supported types and options.

   Types covered:
   - :string, :password, :boolean, :int, :long, :decimal
   - :instant, :enum, :keyword, :symbol, :uuid

   Options covered:
   - table, column-name, max-length
   - owns-ref?, ref, delete-referent?, order-by
   - model->sql-value, sql->model-value"
  (:require
   [com.fulcrologic.rad.attributes :as rad.attr :refer [defattr]]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]))

;; =============================================================================
;; Account - demonstrates basic types and to-one/to-many refs
;; =============================================================================

(defattr account-id :account/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "accounts"})

(defattr account-name :account/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:account/id}
   ::rad.sql/max-length 200})

(defattr account-email :account/email :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:account/id}})

(defattr account-active? :account/active? :boolean
  {::rad.attr/schema :production
   ::rad.attr/identities #{:account/id}
   ::rad.sql/column-name "active"})

;; To-one forward ref (account owns the FK column)
(defattr account-primary-address :account/primary-address :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :address/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:account/id}})

;; To-many reverse ref (addresses have FK to account)
(defattr account-addresses :account/addresses :ref
  {::rad.attr/cardinality :many
   ::rad.attr/target :address/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:account/id}
   ::rad.sql/ref :address/account})

;; Derived data (no schema - not persisted)
(defattr account-locked? :account/locked? :boolean
  {})

(def account-attributes
  [account-id account-name account-email account-active? account-locked?
   account-primary-address account-addresses])

;; =============================================================================
;; Address - demonstrates enum and reverse ref
;; =============================================================================

(defattr addr-id :address/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "addresses"})

(defattr addr-street :address/street :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:address/id}})

(defattr addr-city :address/city :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:address/id}})

(def states #:state {:AZ "Arizona" :KS "Kansas" :MS "Mississippi"})

(defattr addr-state :address/state :enum
  {::rad.attr/enumerated-values (set (keys states))
   ::rad.attr/labels states
   ::rad.attr/schema :production
   ::rad.attr/identities #{:address/id}})

(defattr addr-zip :address/zip :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:address/id}})

;; Reverse ref back to account (for account-addresses to-many)
(defattr addr-account :address/account :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :account/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:address/id}})

(def address-attributes [addr-id addr-street addr-city addr-state addr-zip addr-account])

;; =============================================================================
;; User - basic entity
;; =============================================================================

(defattr user-id :user/id :uuid
  {::rad.attr/schema :production
   ::rad.attr/identity? true
   ::rad.sql/table "users"})

(defattr user-name :user/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:user/id}})

(def user-attributes [user-id user-name])

;; =============================================================================
;; Product - demonstrates numeric types and value transformers
;; =============================================================================

(defattr product-id :product/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "products"})

(defattr product-name :product/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}
   ::rad.sql/max-length 100})

;; :int type -> INTEGER
(defattr product-quantity :product/quantity :int
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

;; :long type -> BIGINT
(defattr product-sku :product/sku :long
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

;; :decimal type -> decimal(20,2)
(defattr product-price :product/price :decimal
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

;; :keyword type -> VARCHAR(200), stored as string
(defattr product-category :product/category :keyword
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

;; :symbol type -> VARCHAR(200), stored as string
(defattr product-status :product/status :symbol
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

;; :password type -> VARCHAR(512)
(defattr product-api-key :product/api-key :password
  {::rad.attr/schema :production
   ::rad.attr/identities #{:product/id}})

(def product-attributes
  [product-id product-name product-quantity product-sku product-price
   product-category product-status product-api-key])

;; =============================================================================
;; Event - demonstrates temporal types
;; =============================================================================

(defattr event-id :event/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "events"})

(defattr event-name :event/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:event/id}})

;; :instant type -> TIMESTAMP WITH TIME ZONE (java.time.Instant)
(defattr event-starts-at :event/starts-at :instant
  {::rad.attr/schema :production
   ::rad.attr/identities #{:event/id}})

;; Another instant for end time
(defattr event-ends-at :event/ends-at :instant
  {::rad.attr/schema :production
   ::rad.attr/identities #{:event/id}})

(def event-attributes [event-id event-name event-starts-at event-ends-at])

;; =============================================================================
;; Category - demonstrates self-referential and delete-referent?
;; =============================================================================

(defattr category-id :category/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "categories"})

(defattr category-name :category/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:category/id}})

;; Self-referential parent (to-one)
(defattr category-parent :category/parent :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :category/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:category/id}})

;; Self-referential children (to-many with ordering)
(defattr category-children :category/children :ref
  {::rad.attr/cardinality :many
   ::rad.attr/target :category/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:category/id}
   ::rad.sql/ref :category/parent
   ::rad.sql/order-by :category/name})

;; Position for ordering
(defattr category-position :category/position :int
  {::rad.attr/schema :production
   ::rad.attr/identities #{:category/id}})

(def category-attributes
  [category-id category-name category-parent category-children category-position])

;; =============================================================================
;; Document - demonstrates owns-ref? and delete-referent?
;; =============================================================================

(defattr document-id :document/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "documents"})

(defattr document-title :document/title :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:document/id}})

;; One-to-one with owned metadata (delete-referent? simulates isComponent)
(defattr document-metadata :document/metadata :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :metadata/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:document/id}
   ::rad.sql/owns-ref? true
   ::rad.sql/delete-referent? true})

(def document-attributes [document-id document-title document-metadata])

;; =============================================================================
;; Metadata - owned by document
;; =============================================================================

(defattr metadata-id :metadata/id :uuid
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "metadata"})

(defattr metadata-author :metadata/author :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:metadata/id}})

(defattr metadata-version :metadata/version :int
  {::rad.attr/schema :production
   ::rad.attr/identities #{:metadata/id}})

;; Reverse ref back to document
(defattr metadata-document :metadata/document :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :document/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:metadata/id}
   ::rad.sql/ref :document/metadata})

(def metadata-attributes [metadata-id metadata-author metadata-version metadata-document])

;; =============================================================================
;; Item - demonstrates :long identity with sequence-based ID generation
;; =============================================================================

(defattr item-id :item/id :long
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "items"})

(defattr item-name :item/name :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:item/id}
   ::rad.sql/max-length 200})

(defattr item-quantity :item/quantity :int
  {::rad.attr/schema :production
   ::rad.attr/identities #{:item/id}})

(defattr item-active :item/active? :boolean
  {::rad.attr/schema :production
   ::rad.attr/identities #{:item/id}
   ::rad.sql/column-name "active"})

;; To-one ref from item to account (item has FK to account)
(defattr item-owner :item/owner :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :account/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:item/id}})

(def item-attributes [item-id item-name item-quantity item-active item-owner])

;; =============================================================================
;; LineItem - another :long identity entity for batch ID generation testing
;; =============================================================================

(defattr line-item-id :line-item/id :long
  {::rad.attr/identity? true
   ::rad.attr/schema :production
   ::rad.sql/table "line_items"})

(defattr line-item-description :line-item/description :string
  {::rad.attr/schema :production
   ::rad.attr/identities #{:line-item/id}})

(defattr line-item-amount :line-item/amount :int
  {::rad.attr/schema :production
   ::rad.attr/identities #{:line-item/id}})

;; Ref to item (for testing cross-entity tempid resolution with int IDs)
(defattr line-item-item :line-item/item :ref
  {::rad.attr/cardinality :one
   ::rad.attr/target :item/id
   ::rad.attr/schema :production
   ::rad.attr/identities #{:line-item/id}})

(def line-item-attributes [line-item-id line-item-description line-item-amount line-item-item])

;; =============================================================================
;; All Attributes
;; =============================================================================

(def all-attributes
  (concat account-attributes
          address-attributes
          user-attributes
          product-attributes
          event-attributes
          category-attributes
          document-attributes
          metadata-attributes
          item-attributes
          line-item-attributes))
