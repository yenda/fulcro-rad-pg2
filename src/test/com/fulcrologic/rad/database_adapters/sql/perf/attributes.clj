(ns com.fulcrologic.rad.database-adapters.sql.perf.attributes
  "Complex attribute model for performance testing.

   Model hierarchy (4 levels deep):
   Organization -> Department -> Project -> Task (-> subtask)

   Relationships exercised:
   - To-one refs (Employee->Address, Dept->Manager, Project->Lead)
   - To-many refs (Org->Depts, Dept->Employees, Project->Tasks)
   - Self-references (Employee->Manager, Task->ParentTask)
   - Shared entities (Address used by Org and Employee)
   - Reverse refs via ::rad.sql/ref"
  (:require
   [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]))

;; =============================================================================
;; Address - Shared by Organization (headquarters) and Employee (home)
;; =============================================================================

(defattr address-id :address/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "addresses"})

(defattr address-street :address/street :string
  {::attr/schema :perf
   ::attr/identities #{:address/id}
   ::rad.sql/max-length 200})

(defattr address-city :address/city :string
  {::attr/schema :perf
   ::attr/identities #{:address/id}
   ::rad.sql/max-length 100})

(defattr address-state :address/state :string
  {::attr/schema :perf
   ::attr/identities #{:address/id}
   ::rad.sql/max-length 50})

(defattr address-postal-code :address/postal-code :string
  {::attr/schema :perf
   ::attr/identities #{:address/id}
   ::rad.sql/column-name "postal_code"
   ::rad.sql/max-length 20})

(defattr address-country :address/country :string
  {::attr/schema :perf
   ::attr/identities #{:address/id}
   ::rad.sql/max-length 50})

(def address-attributes
  [address-id address-street address-city address-state
   address-postal-code address-country])

;; =============================================================================
;; Skill - Many-to-many with Employee via join
;; =============================================================================

(defattr skill-id :skill/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "skills"})

(defattr skill-name :skill/name :string
  {::attr/schema :perf
   ::attr/identities #{:skill/id}
   ::rad.sql/max-length 100})

(defattr skill-category :skill/category :enum
  {::attr/schema :perf
   ::attr/identities #{:skill/id}
   ::attr/enumerated-values #{:category/technical :category/management
                              :category/communication :category/leadership}})

(defattr skill-level :skill/level :enum
  {::attr/schema :perf
   ::attr/identities #{:skill/id}
   ::attr/enumerated-values #{:level/beginner :level/intermediate
                              :level/advanced :level/expert}})

(def skill-attributes
  [skill-id skill-name skill-category skill-level])

;; =============================================================================
;; Organization - Top level entity
;; =============================================================================

(defattr org-id :organization/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "organizations"})

(defattr org-name :organization/name :string
  {::attr/schema :perf
   ::attr/identities #{:organization/id}
   ::rad.sql/max-length 200})

(defattr org-founded :organization/founded :instant
  {::attr/schema :perf
   ::attr/identities #{:organization/id}})

(defattr org-active :organization/active :boolean
  {::attr/schema :perf
   ::attr/identities #{:organization/id}})

(defattr org-employee-count :organization/employee-count :int
  {::attr/schema :perf
   ::attr/identities #{:organization/id}
   ::rad.sql/column-name "employee_count"})

;; To-one: Organization -> Address (headquarters)
(defattr org-headquarters :organization/headquarters :ref
  {::attr/cardinality :one
   ::attr/target :address/id
   ::attr/schema :perf
   ::attr/identities #{:organization/id}})

;; To-many: Organization -> Departments (via reverse ref on department)
(defattr org-departments :organization/departments :ref
  {::attr/cardinality :many
   ::attr/target :department/id
   ::attr/schema :perf
   ::attr/identities #{:organization/id}
   ::rad.sql/ref :department/organization})

(def organization-attributes
  [org-id org-name org-founded org-active org-employee-count
   org-headquarters org-departments])

;; =============================================================================
;; Department - Second level
;; =============================================================================

(defattr dept-id :department/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "departments"})

(defattr dept-name :department/name :string
  {::attr/schema :perf
   ::attr/identities #{:department/id}
   ::rad.sql/max-length 100})

(defattr dept-code :department/code :string
  {::attr/schema :perf
   ::attr/identities #{:department/id}
   ::rad.sql/max-length 10})

(defattr dept-budget :department/budget :decimal
  {::attr/schema :perf
   ::attr/identities #{:department/id}})

(defattr dept-active :department/active :boolean
  {::attr/schema :perf
   ::attr/identities #{:department/id}})

;; To-one: Department -> Organization (parent)
(defattr dept-organization :department/organization :ref
  {::attr/cardinality :one
   ::attr/target :organization/id
   ::attr/schema :perf
   ::attr/identities #{:department/id}})

;; To-one: Department -> Employee (manager)
(defattr dept-manager :department/manager :ref
  {::attr/cardinality :one
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:department/id}})

;; To-many: Department -> Employees (via reverse ref)
(defattr dept-employees :department/employees :ref
  {::attr/cardinality :many
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:department/id}
   ::rad.sql/ref :employee/department})

;; To-many: Department -> Projects (via reverse ref)
(defattr dept-projects :department/projects :ref
  {::attr/cardinality :many
   ::attr/target :project/id
   ::attr/schema :perf
   ::attr/identities #{:department/id}
   ::rad.sql/ref :project/department})

(def department-attributes
  [dept-id dept-name dept-code dept-budget dept-active
   dept-organization dept-manager dept-employees dept-projects])

;; =============================================================================
;; Employee - Third level, with self-reference
;; =============================================================================

(defattr emp-id :employee/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "employees"})

(defattr emp-first-name :employee/first-name :string
  {::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/column-name "first_name"
   ::rad.sql/max-length 50})

(defattr emp-last-name :employee/last-name :string
  {::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/column-name "last_name"
   ::rad.sql/max-length 50})

(defattr emp-email :employee/email :string
  {::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/max-length 100})

(defattr emp-hire-date :employee/hire-date :instant
  {::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/column-name "hire_date"})

(defattr emp-salary :employee/salary :decimal
  {::attr/schema :perf
   ::attr/identities #{:employee/id}})

(defattr emp-active :employee/active :boolean
  {::attr/schema :perf
   ::attr/identities #{:employee/id}})

(defattr emp-title :employee/title :string
  {::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/max-length 100})

;; To-one: Employee -> Department
(defattr emp-department :employee/department :ref
  {::attr/cardinality :one
   ::attr/target :department/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}})

;; To-one: Employee -> Employee (manager) - SELF REFERENCE
(defattr emp-manager :employee/manager :ref
  {::attr/cardinality :one
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}})

;; To-many: Employee -> Employees (direct reports) - SELF REFERENCE REVERSE
(defattr emp-direct-reports :employee/direct-reports :ref
  {::attr/cardinality :many
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/ref :employee/manager})

;; To-one: Employee -> Address (home)
(defattr emp-home-address :employee/home-address :ref
  {::attr/cardinality :one
   ::attr/target :address/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/column-name "home_address"})

;; To-many: Employee -> Skills (via join table)
(defattr emp-skills :employee/skills :ref
  {::attr/cardinality :many
   ::attr/target :skill/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/ref :employee-skill/employee})

;; To-many: Employee -> Tasks (assigned tasks via reverse ref)
(defattr emp-assigned-tasks :employee/assigned-tasks :ref
  {::attr/cardinality :many
   ::attr/target :task/id
   ::attr/schema :perf
   ::attr/identities #{:employee/id}
   ::rad.sql/ref :task/assignee})

(def employee-attributes
  [emp-id emp-first-name emp-last-name emp-email emp-hire-date
   emp-salary emp-active emp-title emp-department emp-manager
   emp-direct-reports emp-home-address emp-skills emp-assigned-tasks])

;; =============================================================================
;; Employee-Skill Join Table (for many-to-many)
;; =============================================================================

(defattr emp-skill-id :employee-skill/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "employee_skills"})

(defattr emp-skill-employee :employee-skill/employee :ref
  {::attr/cardinality :one
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:employee-skill/id}})

(defattr emp-skill-skill :employee-skill/skill :ref
  {::attr/cardinality :one
   ::attr/target :skill/id
   ::attr/schema :perf
   ::attr/identities #{:employee-skill/id}})

(defattr emp-skill-proficiency :employee-skill/proficiency :enum
  {::attr/schema :perf
   ::attr/identities #{:employee-skill/id}
   ::attr/enumerated-values #{:proficiency/learning :proficiency/competent
                              :proficiency/proficient :proficiency/expert}})

(def employee-skill-attributes
  [emp-skill-id emp-skill-employee emp-skill-skill emp-skill-proficiency])

;; =============================================================================
;; Project - Third level under Department
;; =============================================================================

(defattr proj-id :project/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "projects"})

(defattr proj-name :project/name :string
  {::attr/schema :perf
   ::attr/identities #{:project/id}
   ::rad.sql/max-length 200})

(defattr proj-description :project/description :string
  {::attr/schema :perf
   ::attr/identities #{:project/id}
   ::rad.sql/max-length 2000})

(defattr proj-start-date :project/start-date :instant
  {::attr/schema :perf
   ::attr/identities #{:project/id}
   ::rad.sql/column-name "start_date"})

(defattr proj-end-date :project/end-date :instant
  {::attr/schema :perf
   ::attr/identities #{:project/id}
   ::rad.sql/column-name "end_date"})

(defattr proj-status :project/status :enum
  {::attr/schema :perf
   ::attr/identities #{:project/id}
   ::attr/enumerated-values #{:status/planning :status/active
                              :status/on-hold :status/completed
                              :status/cancelled}})

(defattr proj-budget :project/budget :decimal
  {::attr/schema :perf
   ::attr/identities #{:project/id}})

;; To-one: Project -> Department
(defattr proj-department :project/department :ref
  {::attr/cardinality :one
   ::attr/target :department/id
   ::attr/schema :perf
   ::attr/identities #{:project/id}})

;; To-one: Project -> Employee (lead)
(defattr proj-lead :project/lead :ref
  {::attr/cardinality :one
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:project/id}})

;; To-many: Project -> Tasks (via reverse ref)
(defattr proj-tasks :project/tasks :ref
  {::attr/cardinality :many
   ::attr/target :task/id
   ::attr/schema :perf
   ::attr/identities #{:project/id}
   ::rad.sql/ref :task/project})

(def project-attributes
  [proj-id proj-name proj-description proj-start-date proj-end-date
   proj-status proj-budget proj-department proj-lead proj-tasks])

;; =============================================================================
;; Task - Fourth level, with self-reference for subtasks
;; =============================================================================

(defattr task-id :task/id :uuid
  {::attr/identity? true
   ::attr/schema :perf
   ::rad.sql/table "tasks"})

(defattr task-title :task/title :string
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/max-length 200})

(defattr task-description :task/description :string
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/max-length 2000})

(defattr task-priority :task/priority :enum
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::attr/enumerated-values #{:priority/low :priority/medium
                              :priority/high :priority/critical}})

(defattr task-status :task/status :enum
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::attr/enumerated-values #{:status/todo :status/in-progress
                              :status/review :status/done :status/blocked}})

(defattr task-due-date :task/due-date :instant
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/column-name "due_date"})

(defattr task-estimated-hours :task/estimated-hours :int
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/column-name "estimated_hours"})

(defattr task-actual-hours :task/actual-hours :int
  {::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/column-name "actual_hours"})

;; To-one: Task -> Project
(defattr task-project :task/project :ref
  {::attr/cardinality :one
   ::attr/target :project/id
   ::attr/schema :perf
   ::attr/identities #{:task/id}})

;; To-one: Task -> Employee (assignee)
(defattr task-assignee :task/assignee :ref
  {::attr/cardinality :one
   ::attr/target :employee/id
   ::attr/schema :perf
   ::attr/identities #{:task/id}})

;; To-one: Task -> Task (parent) - SELF REFERENCE for subtasks
(defattr task-parent :task/parent :ref
  {::attr/cardinality :one
   ::attr/target :task/id
   ::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/column-name "parent_task"})

;; To-many: Task -> Tasks (subtasks) - SELF REFERENCE REVERSE
(defattr task-subtasks :task/subtasks :ref
  {::attr/cardinality :many
   ::attr/target :task/id
   ::attr/schema :perf
   ::attr/identities #{:task/id}
   ::rad.sql/ref :task/parent})

(def task-attributes
  [task-id task-title task-description task-priority task-status
   task-due-date task-estimated-hours task-actual-hours
   task-project task-assignee task-parent task-subtasks])

;; =============================================================================
;; All Attributes
;; =============================================================================

(def all-attributes
  (concat address-attributes
          skill-attributes
          organization-attributes
          department-attributes
          employee-attributes
          employee-skill-attributes
          project-attributes
          task-attributes))

;; =============================================================================
;; Summary for documentation
;; =============================================================================
;;
;; Entities (8):
;;   Address, Skill, Organization, Department, Employee, EmployeeSkill, Project, Task
;;
;; Relationship depth: 4 levels
;;   Organization -> Department -> Project -> Task (-> subtask)
;;
;; Relationship types:
;;   - To-one: 12 (org->hq, dept->org, dept->manager, emp->dept, emp->manager,
;;                 emp->home, proj->dept, proj->lead, task->proj, task->assignee,
;;                 task->parent, emp-skill->emp, emp-skill->skill)
;;   - To-many: 8 (org->depts, dept->emps, dept->projs, emp->reports, emp->skills,
;;                  emp->tasks, proj->tasks, task->subtasks)
;;   - Self-refs: 2 (emp->manager/reports, task->parent/subtasks)
;;   - Many-to-many via join: 1 (emp<->skill via employee_skills)
;;
;; Attribute types: uuid, string, boolean, int, decimal, instant, enum
