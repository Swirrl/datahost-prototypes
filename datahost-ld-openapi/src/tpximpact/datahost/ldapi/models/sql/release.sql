-- :snip column-spec
:i:column :sql:spec --~(when (:comma? params true) ",")


-- :name -create-observations-table :! :1
/* :require [hugsql.parameters :refer [identifier-param-quote]] */
create table
--~(identifier-param-quote (str "observations::" (:release_id params)) options)
(
    id integer primary key,
    import_id integer not null,
    op int not null,
    :snip*:column-spec
)

-- :name create-imports-table :! :1
create table if not exists imports
(
    -- id integer generated always as identity primary key,-- H2
    id integer primary key autoincrement, -- sqlite
    import_uid varchar(128) unique not null,
    op integer not null,
    status varchar(128) not null,
    created timestamp
)

-- :name -insert-import :! :1
insert into imports (created, status, import_uid, op)
values (CURRENT_TIMESTAMP, 'started', :import_uid, :op)

-- :name select-import :! :1
select * from imports where import_uid = :import_uid

-- :name -create-observations-import-table :! :1
create table
--~(identifier-param-quote (str "import::" (:release_id params)) options)
(
    --id integer generated always as identity primary key, -- H2
    id integer primary key autoincrement, --sqlite
    import_id integer not null,
    op int not null,
    :snip*:column-spec
)

-- :name -import-observations  :! :1
insert into
--~(identifier-param-quote (str "import::" (:release_id params)) options)
(import_id, op, :i*:column-names)
values :t*:observations

-- :name -select-imported-observations :? :*
select :i*:select-columns
from :i:imports-table as selected
where selected.import_id = (select id from imports as i where i.import_uid = :import_uid)

-- :name -select-observations :? :*
select :i*:select-columns
from :i:observations-table as selected
where selected.import_id = (select id from imports as i where i.import_uid = :import_uid)

-- :name -complete-import--copy-records
insert into :i:observations-table (import_id, op, :i*:insert-columns)
select (select id from imports where import_uid = :import_uid), :op, :i*:select-columns
from :i:imports-table

-- :name -complete-import--delete-import-records
delete from :i:imports-table where import_id  = (select id from imports where import_uid = :import_uid)


