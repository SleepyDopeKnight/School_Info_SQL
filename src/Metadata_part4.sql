create database part4;

-------------------------------------------------------------------------------------------
--01
create table tmp
(
    id bigint primary key
);

create table tablename_tmp
(
    id bigint primary key
);

create table tablenametmp
(
    id bigint primary key
);

create table tmp_tablename
(
    id bigint primary key
);

create or replace procedure pcd_drop_tablename() as
$$
declare
    drop varchar;
begin
    for drop
        in select concat('drop table ', table_name) as cmd
           from information_schema.tables
           where table_name ilike 'TableName%'
        loop
            execute drop;
        end loop;
end;
$$ language plpgsql;

call pcd_drop_tablename();

select *
from tmp;
select *
from tmp_tablename;
select *
from tablename_tmp;
select *
from tablenametmp;
-------------------------------------------------------------------------------------------
--02
create or replace function fnc_tmp_fisrt(in jotaro int, out joaline int) returns int as
$$
begin

end;
$$ language plpgsql;

create or replace function fnc_tmp_second(in josef text) returns text as
$$
begin

end;
$$ language plpgsql;

create or replace procedure pcd_find_names_parameters(out count int) as
$$
begin
    count := (select count(*)
              from (select r.routine_name                                               as name,
                           string_agg(concat(p.parameter_name, ' ', p.data_type), ', ') as parameters
                    from information_schema.routines r
                             join information_schema.parameters p on r.specific_name = p.specific_name
                    where r.routine_type = 'FUNCTION'
                      and r.specific_schema = 'public'
                    group by 1) tmp);
end;
$$ language plpgsql;

do
$$
    declare
        count_functions int;
    begin
        call pcd_find_names_parameters(count_functions);
        raise notice 'Founded functions: %', count_functions;
    end;
$$ language plpgsql;
-------------------------------------------------------------------------------------------
--03
create table table_for_triggers
(
    type_event char(1) not null
        CONSTRAINT ch_type_event CHECK ( type_event = 'I' OR type_event = 'U' OR type_event = 'D')
);

create or replace function fnc_trigger_insert() returns trigger as
$$
begin
    insert into table_for_triggers select 'I';
    return null;
end;
$$ language plpgsql;

create or replace trigger trg_insert
    after insert
    on table_for_triggers
    for each row
execute function fnc_trigger_insert();

create or replace function fnc_trigger_update() returns trigger as
$$
begin
    insert into table_for_triggers select 'U';
    return null;
end;
$$ language plpgsql;

create or replace trigger trg_update
    after update
    on table_for_triggers
    for each row
execute function fnc_trigger_update();

create or replace function fnc_trigger_delete() returns trigger as
$$
begin
    insert into table_for_triggers select 'D';
    return null;
end;
$$ language plpgsql;

create or replace trigger trg_delete
    after delete
    on table_for_triggers
    for each row
execute function fnc_trigger_delete();

create or replace procedure pcd_drop_triggers(out count int) as
$$
declare
    drop varchar;
begin
    count := (select count(*) from information_schema.triggers);
    if count <> 0 then
        for drop
            in select concat('drop trigger ', trigger_name, ' on ', event_object_table) as cmd
               from information_schema.triggers
            loop
                execute drop;
            end loop;
    end if;
end;
$$ language plpgsql;

do
$$
    declare
        count_of_drops int;
    begin
        call pcd_drop_triggers(count_of_drops);
        raise notice 'Dropped triggers: %';
    end;
$$ language plpgsql;
-------------------------------------------------------------------------------------------
--04

create or replace procedure pcd_finding_names_and_types(in ref refcursor, in pattern text) as
$$
begin
    open ref for
        select routine_name as name, routine_type as type
        from information_schema.routines
        where specific_schema = 'public'
          and routine_definition like concat('%', pattern, '%');
end;
$$ language plpgsql;

begin;
call pcd_finding_names_and_types('cursor_name', 'trigger');
fetch all in "cursor_name";
end;