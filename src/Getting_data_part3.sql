-------------------------------------------------------------------------------------------
--01                  rabotaet

create or replace function fnc_points_amount()
    returns table
            (
                peer1        varchar,
                peer2        varchar,
                pointsamount integer
            )
as
$$
with tp as (select tp1.checkingpeer, tp1.checkedpeer, tp1.pointsamount
            from transferredpoints tp1
                     join transferredpoints tp2
                          on tp2.checkingpeer = tp1.checkedpeer and tp2.checkedpeer = tp1.checkingpeer
            where tp1.id < tp2.id)

    (select tmp.checkingpeer, tmp.checkedpeer, sum(tmp.pointsamount)
     from (select t.checkedpeer, t.checkingpeer, t.pointsamount
           from transferredpoints t
           union
           select tp.checkingpeer, tp.checkedpeer, -tp.pointsamount
           from tp) tmp
     group by 1, 2)
except
select tp.checkingpeer, tp.checkedpeer, tp.pointsamount
from tp;
$$ language sql;

select *
from fnc_points_amount();
-------------------------------------------------------------------------------------------
--02                  rabotaet

create or replace function fnc_info_peer_success_project()
    returns table
            (
                peer     varchar,
                task     varchar,
                xpamount integer
            )
as
$$
select c.peer, c.task, x.xpamount
from checks c
         inner join xp x on c.id = x."Check"
         inner join p2p p on c.id = p."Check"
where p.state = 'Success'
$$ language sql;

select *
from fnc_info_peer_success_project();

-------------------------------------------------------------------------------------------
--03                  rabotaet

create or replace function fnc_not_come_out_peers(IN "Date" date)
    returns table
            (
                peer varchar
            )
as
$$
(select peer
 from timetracking
 where state = 1
   and date = "Date"
 group by peer)
except
(select peer
 from timetracking
 where state = 2
   and date = "Date"
 group by peer);
$$ language sql;

select *
from fnc_not_come_out_peers('2022-12-01');

-------------------------------------------------------------------------------------------
--04                  rabotaet
create or replace procedure pcd_info_percent_success_projects(in ref refcursor)
    language plpgsql as
$$
begin
    open ref for
        with SuccessfulChecks as (select count(*) as success
                                  from fnc_success_checks()),
             UnsuccessfulChecks as (select SUM(fail) as fail
                                    from (select count(*) as fail
                                          from verter
                                          where state = 'Failure'
                                          union
                                          select count(*) as fail
                                          from p2p
                                          where state = 'Failure') failed)
        select round((success / (success + fail)) * 100, 2) as SuccessfulChecks,
               round((fail / (success + fail)) * 100, 2)    as UnsuccessfulChecks
        from SuccessfulChecks,
             UnsuccessfulChecks;
end;
$$;

begin;
call pcd_info_percent_success_projects('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--05                  rabotaet

create or replace procedure pcd_PointsChange(in ref refcursor)
    language plpgsql as
$$
begin
    open ref for
        with amount_received as (select checkingpeer      as Peer,
                                        sum(pointsamount) as PointsChange
                                 from transferredpoints
                                 group by checkingpeer
                                 order by checkingpeer),
             amount_spent as (select checkedpeer       as Peer,
                                     sum(pointsamount) as PointsChange
                              from transferredpoints
                              group by checkedpeer
                              order by checkedpeer)

        select ar.Peer, ar.PointsChange - "as".PointsChange as PointsChange
        from amount_received ar
                 inner join amount_spent "as" on ar.Peer = "as".Peer;
end;
$$;

begin;
call pcd_PointsChange('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--06                   rabotaet
create or replace procedure pcd_PointsChange_2(in ref refcursor) as
$$
begin
    open ref for
        with amount_received as (select peer1             as Peer,
                                        sum(pointsamount) as PointsChange
                                 from fnc_points_amount()
                                 group by peer1
                                 order by peer1),
             amount_spent as (select peer2             as Peer,
                                     sum(pointsamount) as PointsChange
                              from fnc_points_amount()
                              group by peer2
                              order by peer2),
             res as (select case when ar.Peer is null then "as".peer else ar.Peer end,
                            case
                                when ar.PointsChange - "as".PointsChange is null
                                    then ar.PointsChange
                                else ar.PointsChange - "as".PointsChange
                                end as PointsChange
                     from amount_received ar
                              full join amount_spent "as" on ar.Peer = "as".Peer
                     union
                     select case when ar.Peer is null then "as".peer else ar.Peer end,
                            case
                                when ar.PointsChange - "as".PointsChange is null
                                    then "as".PointsChange
                                else ar.PointsChange - "as".PointsChange
                                end as PointsChange
                     from amount_received ar
                              full join amount_spent "as" on ar.Peer = "as".Peer)

        select *
        from res
        where res.PointsChange is not null
        order by 1;
end;
$$ language plpgsql;

begin;
call pcd_most_reviewd_task_of_the_day('cursor_name');
fetch all in "cursor_name";
end;

-------------------------------------------------------------------------------------------
--07                  rabotaet
create or replace procedure pcd_most_reviewd_task_of_the_day(in ref refcursor) as
$$
begin
    open ref for
        with count_tasks as (select task, date, count(*)
                             from checks c
                             group by 1, 2),
             max_count_tasks as (select task,
                                        date,
                                        max(count)
                                 from count_tasks
                                 group by 1, 2)

        select date as day, task
        from max_count_tasks;
end;
$$ language plpgsql;

begin;
call pcd_most_reviewd_task_of_the_day('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--08                  rabotaet

create or replace procedure pcd_duration_of_last_p2p_check(in ref refcursor)
    language plpgsql as
$$
begin
    open ref for
        with last_check as (select "Check" as check_id
                            from p2p
                            where id = (select max(id)
                                        from p2p
                                        where state = 'Start'))
        select (max("Time") - min("Time"))::time as times
        from last_check lc
                 inner join p2p on lc.check_id = p2p."Check";
end;
$$;

begin;
call pcd_duration_of_last_p2p_check('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--09                  rabotaet

create or replace procedure pcd_completed_block(in ref refcursor, in blockname varchar) as
$$
begin
    open ref for
        with list_of_tasks as (select count(title)
                               from tasks
                               where title like blockname),
             foo as (select distinct checkedpeer, task
                      from (select "Check", Peer as CheckedPeer, Task
                            from p2p
                                     inner join checks c on c.id = p2p."Check"
                            where state = 'Success'
                              and task like blockname
                            except all
                            (select "Check", Peer as CheckedPeer, Task
                             from verter
                                      inner join checks c on c.id = verter."Check"
                             where state = 'Failure'
                             union all
                             select "Check", Peer as CheckedPeer, Task
                             from p2p
                                      inner join checks c on c.id = p2p."Check"
                             where state = 'Failure')) success),
             tmp as (select CheckedPeer, count(foo.task) as completed, list_of_tasks.count as list
                     from foo,
                          list_of_tasks
                     group by 1, 3)

        select peer, max(date) as day
        from checks
                 join tmp on tmp.CheckedPeer = checks.peer
        where checks.peer = tmp.CheckedPeer
          and checks.task like blockname
          and tmp.list = tmp.completed
        group by 1;
end;
$$ language plpgsql;

begin;
call pcd_completed_block('cursor_name', 'CPP%');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--010                  rabotaet

create or replace procedure pcd_recommendation_list(in ref refcursor)
as
$$
begin
    open ref for
        with list_of_recommendation as (select peer1, recommendedpeer, count(peer) as count
                                        from friends f
                                                 inner join recommendations r on f.peer2 = r.peer
                                        where peer1 <> r.recommendedpeer
                                        group by peer1, recommendedpeer
                                        order by 1, count desc, 2),
             table_of_max_count_recommendation as (select peer1, max(count) as max_count
                                                   from list_of_recommendation
                                                   group by peer1)

        select mcr.peer1 as Peer, recommendedpeer as RecommendedPeer
        from table_of_max_count_recommendation mcr
                 inner join list_of_recommendation lor on lor.count = mcr.max_count and
                                                          lor.peer1 = mcr.peer1
        order by Peer;
end;
$$ language plpgsql;

begin;
call pcd_recommendation_list('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--011                   rabotaet
-- процент
create or replace procedure pcd_peers_started_two_blocks(in ref refcursor, blockname_1 varchar, blockname_2 varchar) as
$$
begin
    open ref for
        with first_block as (select distinct peer
                             from checks
                             where task like blockname_1),
             second_block as (select distinct peer
                              from checks
                              where task like blockname_2),
             both_blocks as (select distinct peer
                             from first_block
                             intersect
                             select distinct peer
                             from second_block),
             didnt_start as (select nickname as peer
                             from peers
                             except
                             (select distinct peer
                              from first_block
                              union
                              select distinct peer
                              from second_block))

        select (select count(peer) from first_block) * 100 / count(nickname)  as StartedBlock1,
               (select count(peer) from second_block) * 100 / count(nickname) as StartedBlock2,
               (select count(peer) from both_blocks) * 100 / count(nickname)  as StartedBothBlocks,
               (select count(peer) from didnt_start) * 100 / count(nickname)  as DidntStartAnyBlock
        from peers;
end;
$$ language plpgsql;

begin;
call pcd_peers_started_two_blocks('cursor_name', 'A%', 'A%');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--012                  rabotaet

create or replace procedure pcd_most_friendly_peers(in ref refcursor, in ccount integer)
as
$$
begin
    open ref for
        select peer1 as peer, count(*) as FriendsCount
        from (select peer1
              from friends
              union all
              select peer2
              from friends) as tmp
        group by peer1
        order by FriendsCount desc
        limit ccount;
end;
$$ language plpgsql;

begin;
call pcd_most_friendly_peers('cursor_name', '2');
fetch all in "cursor_name";
end;

begin;
call pcd_most_friendly_peers('cursor_name', '5');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--013                  rabotaet

create or replace procedure pcd_success_reviews_on_birthday(in ref refcursor) as
$$
begin
    open ref for
        with succ_and_fail_stat as (select count(peer)
                                           filter (where p2p.state = 'Success' and v.state <> 'Failure') as success,
                                           count(peer) filter (where (p2p.state = 'Success' and v.state = 'Failure') or
                                                                     (p2p.state = 'Failure'))            as fail
                                    from checks c
                                             inner join verter v on c.id = v."Check"
                                             inner join p2p on c.id = p2p."Check"
                                             inner join peers p on p.nickname = c.peer
                                    where extract(month from c.date) = extract(month from p.birthday)
                                      and extract(day from c.date) = extract(day from p.birthday))

        select round(100 * success / (success + fail)) as SuccessfulChecks,
               round(100 * fail / (success + fail))    as UnsuccessfulChecks
        from succ_and_fail_stat;
end;
$$ language plpgsql;

begin;
call pcd_success_reviews_on_birthday('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--014                  rabotaet

create or replace procedure pcd_xp_info(in ref refcursor)
as
$$
begin
    open ref for
        select peer, sum(xp) as xp
        from (select peer, t.title, max(xpamount) as xp
              from xp
                       inner join checks c on c.id = xp."Check"
                       inner join tasks t on t.title = c.task
              group by peer, title) tmp
        group by peer
        order by xp desc;
end;
$$ language plpgsql;

begin;
call pcd_xp_info('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--015                  rabotaet

create or replace procedure pcd_completed_task(in ref refcursor, in first_task text, in second_task text,
                                               in third_task text) as
$$
begin
    open ref for
        with success as (select "Check", Peer as CheckedPeer, Task
                         from p2p
                                  inner join checks c on c.id = p2p."Check"
                         where state = 'Success'
                         except all
                         (select "Check", Peer as CheckedPeer, Task
                          from verter
                                   inner join checks c on c.id = verter."Check"
                          where state = 'Failure'
                          union all
                          select "Check", Peer as CheckedPeer, Task
                          from p2p
                                   inner join checks c on c.id = p2p."Check"
                          where state = 'Failure'))

        select distinct s1.CheckedPeer
        from success s1
                 full join success s2 on s2.CheckedPeer = s1.CheckedPeer
                 full join success s3 on s3.CheckedPeer = s1.CheckedPeer
        where s1.task = first_task
          and s2.task = second_task
          and s3.task <> third_task;
end;
$$ language plpgsql;

begin;
call pcd_completed_task('cursor_name', 'CPP4_s21_matrix', 'CPP5_s21_containers', 'CPP6_s21_calc');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--016                  rabotaet

create or replace function fnc_counting(in ttasks text) returns integer as
$$
with recursive count_tasks as (select title, parenttask, 0 as count
                               from tasks
                               where title = ttasks
                               union
                               select tasks.title, tasks.parenttask, count_tasks.count + 1
                               from tasks
                                        join count_tasks on tasks.title = count_tasks.parenttask)

select max(count)
from count_tasks
$$ language sql;

create or replace procedure pcd_count_complete_for_access_task(in ref refcursor) as
$$
begin
    open ref for
        select tasks.title as task, fnc_counting(tasks.title) as PrevCount from tasks;
end;
$$ language plpgsql;

begin;
call pcd_count_complete_for_access_task('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--017

with temp as (select *,
                case when state = 'Success' then row_number() over (partition by state, date order by "Check", date, "Time")
                    else 0
                    end as tmp
from checks
inner join p2p p on checks.id = p."Check"
where state <> 'Start'
order by "Check", date, "Time")


select *
 from temp

select date, max(tmp)
from temp
where state = 'Success'
group by 1

with success as (select "Check", Peer as CheckedPeer, state, date, Task
                         from p2p
                                  inner join checks c on c.id = p2p."Check"
                         where state = 'Success'
                         union all
                         (select "Check", Peer as CheckedPeer, state, date, Task
                          from verter
                                   inner join checks c on c.id = verter."Check"
                          where state = 'Failure'
                          union all
                          select "Check", Peer  as CheckedPeer, state, date, Task
                          from p2p
                                   inner join checks c on c.id = p2p."Check"
                          where state = 'Failure') order by "Check"),
    foo as (select distinct on ("Check")"Check",CheckedPeer,state,date,task, case when state = 'Success' then
        row_number() over (partition by state, date order by "Check", date)
                    else 0
                    end as count from success
order by 1 desc)

with recursive success as (select "Check", Peer as CheckedPeer, state, date, Task, row_number() over (partition by state, date) as rn
                         from p2p
                                  inner join checks c on c.id = p2p."Check"
                         where state = 'Success'
                         union all
                         (select "Check", Peer as CheckedPeer, state, date, Task, row_number() over (partition by state, date)
                          from verter
                                   inner join checks c on c.id = verter."Check"
                          where state = 'Failure'
                          union all
                          select "Check", Peer  as CheckedPeer, state, date, Task, row_number() over (partition by state, date)
                          from p2p
                                   inner join checks c on c.id = p2p."Check"
                          where state = 'Failure') order by "Check"),
    foo as (select "Check", checkedpeer, state, date, task, rn, 1 as tmp from success where rn = 1
            union all
            select sc."Check", sc.checkedpeer, sc.state, sc.date, sc.task, sc.rn, case
                when sc.state = foo.state then tmp + 1 else 1 end
                from foo, success sc where foo.rn = sc.rn - 1)

select distinct on ("Check") * from foo order by 1;
-------------------------------------------------------------------------------------------
create or replace procedure pcd_lucky_days(in ref refcursor, in N int) as
$$
begin
    open ref for
        with success as (select "Check", Peer as CheckedPeer, state, date, Task
                         from p2p
                                  inner join checks c on c.id = p2p."Check"
                         where state = 'Success'
                         union all
                         (select "Check", Peer as CheckedPeer, state, date, Task
                          from verter
                                   inner join checks c on c.id = verter."Check"
                          where state = 'Failure'
                          union all
                          select "Check", Peer as CheckedPeer, state, date, Task
                          from p2p
                                   inner join checks c on c.id = p2p."Check"
                          where state = 'Failure')
                         order by "Check"),
             foo as (select distinct on ("Check") "Check",
                                                  CheckedPeer,
                                                  state,
                                                  date,
                                                  task,
                                                  case
                                                      when state = 'Success'
                                                          then row_number() over (partition by state, date order by "Check", date)
                                                      else 0
                                                      end as count
                     from success
                     order by 1 desc)

        select date
        from (select date, max(count) as count
              from foo
                       join xp on xp."Check" = foo."Check"
                       join tasks on tasks.title = foo.task
              where state = 'Success'
                and xp.xpamount > tasks.maxxp * 0.8
              group by 1
              order by 1) tmp
        where count >= N;
end;
$$ language plpgsql;

begin;
call pcd_lucky_days('cursor_name', '1');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--018                  rabotaet

create or replace procedure pcd_leader_of_projects(in ref refcursor)
as
$$
begin
    open ref for
        select peer, count(task) as completed
        from checks
        group by peer
        order by 2 desc
        limit 1;
end;
$$ language plpgsql;

begin;
call pcd_leader_of_projects('cursor_name');
fetch all in "cursor_name";
end;

-------------------------------------------------------------------------------------------
--019                  rabotaet
create or replace procedure pcd_peer_with_higher_xp(in ref refcursor)
as
$$
begin
    open ref for
        with tmp as ((select peer, task
                       from xp
                                inner join checks c on c.id = xp."Check"
                       order by 2 desc)
                      except
                      ((select peer, task
                        from xp
                                 inner join checks c on c.id = xp."Check")
                       except
                       (select distinct peer, task
                        from xp
                                 inner join checks c on c.id = xp."Check"))),
             foo as (select distinct tmp.peer, tmp.task, max(c.date) as date
                        from tmp
                               inner join checks c on tmp.task = c.task and tmp.peer = c.peer
                               inner join xp x on c.id = x."Check" and tmp.task = c.task and tmp.peer = c.peer
                      group by 1, 2)

        select distinct foo.peer, sum(xpamount)
        from foo
                 inner join checks c on foo.peer = c.peer
                 inner join xp x2 on c.id = x2."Check"
        where c.date = foo.date
        group by 1
        order by 2 desc
        limit 1;
end;
$$ language plpgsql;

begin;
call pcd_peer_with_higher_xp('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--20                   rabotaet
create or replace procedure pcd_max_time_in_campus(in ref refcursor) as
$$
begin
    open ref for
        with t1 as (select id, peer, time from timetracking where state = 1 and date = now()::date),
             t2 as (select id, peer, time from timetracking where state = 2 and date = now()::date),
             t12 as (select distinct on (t1.id) t1.id   as id_1,
                                                t1.peer as peer_1,
                                                t1.time as time_1,
                                                t2.id   as id_2,
                                                t2.peer as peer_2,
                                                t2.time as time_2
                     from t1
                              inner join t2 on t1.peer = t2.peer and t1.time < t2.time)

        select peer_1 as peer
        from t12
        group by 1
        having (sum(time_2 - time_1)::time) = (select (sum(time_2 - time_1)::time) as t3
                                               from t12
                                               group by peer_1
                                               order by 1 desc
                                               limit 1);
end;
$$ language plpgsql;

begin;
call pcd_max_time_in_campus('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--21                   rabotaet

create or replace procedure pcd_coming_in_campus(in ref refcursor, in ttime time, in n integer) as
$$
begin
    open ref for
        select peer
        from (select peer, date, min(time) from timetracking where state = '1' and time < ttime group by 1, 2) tmp
        group by 1
        having count(*) >= n;
end;
$$ language plpgsql;

begin;
call pcd_coming_in_campus('cursor_name', '19:00:00', '1');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--22                   rabotaet

create or replace procedure pcd_sum_out_of_campus(in ref refcursor, in n integer, in m integer) as
$$
begin
    open ref for
        select peer
        from (select peer, "date", count(*) - 1 as count
              from timetracking
              where "state" = '2'
                and date > (now()::date - n)
              group by 1, 2) tmp
        group by peer
        having sum(count) > m;
end;
$$ language plpgsql;

begin;
call pcd_sum_out_of_campus('cursor_name', '90', '0');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--023                   rabotaet

create or replace procedure pcd_last_visited_peer(in ref refcursor) as
$$
begin
    open ref for
        select peer
        from timetracking
        where state = 1
          and date = current_date
        order by time desc
        limit 1;
end;
$$ language plpgsql;

begin;
call pcd_last_visited_peer('cursor_name');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--24                   rabotaet

create or replace procedure pcd_yesterday_out_of_campus(in ref refcursor, in N integer) as
$$
begin
    open ref for
        with "in" as (select row_number() over () as id, peer, date, time
                      from timetracking tt
                      where state = '1'
                        and date = current_date - 1
                        and not time = (select min(time)
                                        from timetracking tt1
                                        where tt.peer = tt1.peer
                                          and tt.date = tt1.date)
                      order by 2, 4),
             out as (select row_number() over () as id, peer, date, time
                     from timetracking tt
                     where state = '2'
                       and date = current_date - 1
                       and not time =
                               (select max(time) from timetracking tt1 where tt.peer = tt1.peer and tt.date = tt1.date)
                     order by 2, 4),
             "all" as (select "in".peer, "in".time as come, out.time as out
                       from "in"
                                join out on "in".id = out.id and "in".date = out.date)

        select peer
        from "all"
        group by 1
        having sum((come - out))::time > make_time(N / 60, N - N / 60 * 60, 0.);
end;
$$ language plpgsql;

begin;
call pcd_yesterday_out_of_campus('cursor_name', '90');
fetch all in "cursor_name";
end;
-------------------------------------------------------------------------------------------
--25 todo

create or replace function fnc_count_of_visits(in ppeer text, in ttime time) returns integer as
$$
select count(*)
from (select peer
      from timetracking
      where peer = ppeer
        and time < ttime
        and state = '1') tmp
$$
    language sql;

create or replace procedure pcd_early_visits_for_months(in ref refcursor) as
$$
begin
    open ref for
        with months as (select months::date
                        from generate_series('2022-01-01', '2022-12-01', interval '1 month') as months),
             visits as (select to_char(months::date, 'Month')                         as month,
                               sum(fnc_count_of_visits(p.nickname::text, '24:00:00')) as visits,
                               sum(fnc_count_of_visits(p.nickname::text, '12:00:00')) as early_visits
                        from months
                                 full join peers p on extract(month from months) = extract(month from p.birthday::date)
                        group by 1)

        select month, case when visits = 0 then 0 else early_visits * 100 / visits end as EarlyEntries
        from visits;
end;
$$ language plpgsql;

begin;
call pcd_early_visits_for_months('cursor_name');
fetch all in "cursor_name";
end;
----------------------------- additional


select *
from fnc_success_checks();
