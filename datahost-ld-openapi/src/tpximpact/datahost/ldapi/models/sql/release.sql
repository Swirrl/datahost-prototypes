-- :snip column-spec
:i:column :sql:spec --~(when (:comma? params true) ",")

-- :snip import-id
(select id from imports where import_uid = :import-uid and release_uri = :release-uri)

-- :snip all-commit-ids
with recursive all_commits as (
    select id, parent_id, op, uid
    from commits
    where id = (select id from commits where uid = :uid)
    union
    select c.id, c.parent_id, c.op, c.uid
    from commits c
    inner join all_commits ac on c.id = ac.parent_id
    where c.release_uri = :release-uri
)

-- :snip -commit-info
-- :doc get joined import and commit data for :change-uri. Returns table of [import_id, commits.op]
(
     select i.id as import_id, c.op
     from imports i
     inner join commits c
     on i.import_uid = c.import_uid
     where     c.release_uri = :release-uri
           and i.release_uri = :release-uri
           and
--~(if-let [commit-id (:commit-id params)] "c.id = :commit-id" "c.uid = :commit-uri")
)

-- :name -create-observations-table :! :1
/* :require [hugsql.parameters :refer [identifier-param-quote]] */
create table
--~(identifier-param-quote (str "observations::" (:release_id params)) options)
(
    id integer primary key,
    import_id integer not null,
    coords integer not null,
    synth_id integer not null,
    :snip*:column-spec,
    unique (import_id,coords)
)

-- :name -create-observations-import-table :! :1
create table
--~(identifier-param-quote (str "import::" (:release_id params)) options)
(
    --id integer generated always as identity primary key, -- H2
    id integer primary key autoincrement, --sqlite
    import_id integer not null,
    coords integer not null,
    synth_id integer not null,
    :snip*:column-spec,
     unique (import_id,coords)
)

-- :name create-temporary-snapshot-table :! :1
-- :doc Creates a table with [id, import_id, coords, synth_id] columns
create temp table :i:table-name
-- ---(identifier-param-quote (str (:table-name params)) options)
(
    -- we no longer want to generate the id, we will supply it
    id integer primary key not null,
    import_id integer not null,
    coords integer not null,
    synth_id integer not null,
    new_id integer,
    unique (import_id,coords)
)

-- :name create-imports-table :! :1
create table if not exists imports
(
-- id integer generated always as identity primary key,-- H2
    id integer primary key autoincrement, -- sqlite
    import_uid varchar(128) not null,
    -- status: 'created' | 'failed' | 'completed'
    status varchar(128) not null,
    created timestamp not null,
    updated timestamp,
    release_uri not null
)

-- :name -insert-import :! :1
with import_ids as :snip:import-id
insert into imports (created, status, import_uid, release_uri)
select CURRENT_TIMESTAMP, 'started', :import-uid, :release-uri
where not exists (select 1 from import_ids)
--(select 1 from imports
--                  where import_uid = :import-uid and release_uri = :release-uri)

-- :name select-import :! :1
select * from imports
where import_uid = :import-uid and release_uri =
--~(format "'%s'" (:release-uri (:store options)))


-- :name update-import-status !: :1
with import_ids as :snip:import-id
update imports
set status = :status, updated = CURRENT_TIMESTAMP
where id = (select id from import_ids)
and status != 'completed'

-- :name create-commits-table :! :1
create table if not exists commits (
       -- id integer generated always as identity primary key, -- H2
       id integer primary key autoincrement,
       uid text not null,
       import_uid text not null, -- TODO: change to FK?
       revision_id integer not null,
       change_id integer not null, -- 'commit-id in triplestore'
       parent_id integer,
       op integer not null,
       release_uri not null, -- TODO: revise, column probably not needed
       unique (uid)
);

-- :name create-commits-by-uid-index :! :1
create index if not exists commits_by_uid on commits (uid);

-- :name create-commits-by-import-uid-index :! :1
create index if not exists commits_by_import_uid on commits (import_uid);

-- :name create-commits-by-revision-id-index :! :1
create index if not exists commits_by_revision_id on commits (revision_id);

-- :name create-commits-by-change-id-index :! :1
create index if not exists commits_by_change_id on commits (change_id);

-- :name insert-commit :! :1
-- :doc Inserts a commit record into given release
with parent_commits as (
    select id, revision_id, change_id
    from commits
    where release_uri = :release-uri
           and ((revision_id = :revision-id and change_id = :change-id - 1)
                or
                (revision_id < :revision-id))
           -- probably can add a guard against import_uid && op (same change twice in a row)
    order by revision_id desc, change_id desc
    limit 1
)
insert into commits (uid, import_uid, parent_id, revision_id, change_id, op, release_uri)
select
    :uid,
     :import_uid,
     (select id from parent_commits),
     :revision-id,
     :change-id,
     :op,
     :release-uri
where not exists (select 1 from commits
                  where
                        revision_id = :revision-id
                    and change_id = :change-id
                    and uid = :uid
                    limit 1)

-- :name -import-observations  :! :1
insert into
--~(identifier-param-quote (str "import::" (:release_id params)) options)
(import_id, coords, synth_id, :i*:column-names)
values :t*:observations

-- :name -select-imported-observations :? :*
with import_ids as :snip:import-id
select :i*:select-columns
from :i:imports-table as selected
where selected.import_id = (select id from import_ids)

-- :name -select-observations :? :*
with import_ids as :snip:import-id
select :i*:select-columns
from :i:observations-table as selected
where selected.import_id = (select id from import_ids)

-- :name -complete-import--copy-records :! :n
with import_id as :snip:import-id
insert into :i:observations-table (import_id, coords, synth_id, :i*:insert-columns)
select (select id from import_id), coords, synth_id, :i*:select-columns
from :i:imports-table

-- :name -complete-import--delete-import-records
with import_ids as :snip:import-id
delete from :i:imports-table where import_id = (select id from import_ids)

-- :name -get-commit-ids  :? :*
-- :doc returns {:id int, parent_id int, :op :int}
:snip:all-commit-ids
select id, parent_id, op, uid from all_commits

-- :name -commit-op-append :? :*
-- :doc TODO
with commit_info as :snip:-commit-info,
duplicates as (
 select
    o.id as dup_id,
    o.coords as dup_coords
 from :i:observations-table o
 inner join :i:snapshot-table s
 on o.coords = s.coords
 where o.import_id != s.import_id
),
valid_ids as (
 select id
 from :i:observations-table
 where import_id = (select import_id from commit_info )
 except
 select dup_id from duplicates
)
insert into :i:snapshot-table (id, import_id, coords, synth_id)
select o.id, (select import_id from commit_info) as import_id, o.coords, o.synth_id
from :i:observations-table o
inner join valid_ids ids
on ids.id = o.id

-- :name -commit-op-retract :? :*
-- :doc TODO
with commit_info as :snip:-commit-info,
coords_to_remove as (
 select coords, synth_id
 from :i:snapshot-table s
 intersect
 select coords, synth_id
 from :i:observations-table o
 where      o.import_id = (select import_id from commit_info)
)
delete from :i:snapshot-table
where coords in (select coords  from coords_to_remove)

-- :name -commit-op-correct*
-- :doc TODO
with commit_info as :snip:-commit-info,
measures_to_update as (
 select o.id, o.coords, o.synth_id
 from :i:observations-table o
 inner join :i:snapshot-table s
 on s.coords = o.coords
 where o.import_id = (select import_id from commit_info)
 and s.synth_id != o.synth_id
)
update :i:snapshot-table
set new_id = (select id from measures_to_update m where m.coords = coords)

-- :name -create-corrections-scratch-table
-- :doc TODO
create temp table :i:table-name (
   old_id integer not null,
   new_id integer not null,
   unique (old_id, new_id)
)

-- :name -populate-corrections-scratch-table
-- :doc TODO
with commit_info as :snip:-commit-info,
measures_to_update as (
 select s.id as old_id, o.id as new_id, o.coords, o.synth_id
 from :i:observations-table o
 inner join :i:snapshot-table s
 on s.coords = o.coords
 where o.import_id = (select import_id from commit_info)
 and s.synth_id != o.synth_id
)
insert into :i:table-name (old_id, new_id)
select old_id, new_id
from measures_to_update

-- :name -commit-op-correct--delete-stale-records
-- :doc TODO
delete from :i:snapshot-table
where id in (select old_id from :i:table-name)

-- :name -commit-op-correct--insert-updated-records
-- :doc TODO
with commit_info as :snip:-commit-info
insert into :i:snapshot-table (id, import_id, coords, synth_id)
select scratch.new_id, o.import_id, o.coords, o.synth_id
from :i:table-name scratch
inner join :i:observations-table o
on o.id = scratch.new_id
where o.import_id = (select import_id from commit_info)

-- :name -select-commit-observations :? :*
-- :doc Get observations belonging to a commit specified by :commit-uri
with commit_info as :snip:-commit-info, -- gives us: [import_id, commits.op]
selected as (
     select :i*:select-columns
     from :i:observations-table as selected
     where import_id = (select import_id from commit_info)
)
select * from selected
join commit_info c
on import_id = c.import_id

-- :name -select-observation-snapshot-data
-- :doc needs: :snapshot-table, :release-uri, :commit-uri, and snippets: :-commit-info
with commit_info as :snip:-commit-info
insert into :i:snapshot-table (id, import_id, coords, synth_id) 
select id, import_id, coords, synth_id
from :i:observations-table
where import_id = (select import_id from commit_info)

-- :name -materialise-snapshot
-- :doc TODO
select :i*:select-columns -- X as selected.X
from :i:snapshot-table s
inner join :i:observations-table selected
on s.id = selected.id
order by s.id asc

--:name -drop-table
drop table :i:table-name

