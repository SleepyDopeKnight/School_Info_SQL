------------------------------------------------------------------------------------------------------------------------
--01
create or replace function fnc_p2p_check_started(in CheckedPeer varchar, in pCheckingPeer varchar, in TitleTask varchar) returns bool as
$$
begin
    return case
               when exists
                   (with tmp as (select max(p2p.id) as id, p2p.checkingpeer, p2p.state
                                  from "checks" c
                                           left join p2p on c.id = p2p."Check" and c.peer = CheckedPeer and c.task = TitleTask
                                  where checkingpeer = pCheckingPeer
                                  group by p2p.id
                                  order by 1 desc
                                  limit 1)

                    select state
                    from tmp
                    where state = 'Start') then true
               else false
        end;
end;
$$ language plpgsql;

create or replace procedure pcd_P2P_check(in CheckedPeer varchar,
                                          in CheckingPeer varchar,
                                          in TitleTask varchar,
                                          in State varchar,
                                          in "Time" time
)
    language plpgsql as
$$
begin
    if State = 'Start' and (select *
                            from fnc_p2p_check_started(CAST(CheckedPeer as varchar),
                                                       CAST(CheckingPeer as varchar), CAST(TitleTask as varchar))) =
                           false then
        insert into checks
        values ((select MAX(id) + 1 from checks),
                CheckedPeer,
                TitleTask,
                current_date);
    end if;

    if ((select *
         from fnc_p2p_check_started(CAST(CheckedPeer as varchar),
                                    CAST(CheckingPeer as varchar), CAST(TitleTask as varchar))) = true and
        State <> 'Start') or
       (State = 'Start' and (select *
                             from fnc_p2p_check_started(CAST(CheckedPeer as varchar), CAST(CheckingPeer as varchar),
                                                        CAST(TitleTask as varchar))) = false) then
        insert into p2p
        values ((select MAX(id) + 1 from p2p),
                (select MAX(ID) from checks),
                CheckingPeer,
                State,
                "Time");
    end if;
end;
$$;

call pcd_P2P_check('Valera', 'Feodor', 'CPP5_s21_containers', 'Start', '11:46:00'::time);
call pcd_P2P_check('Valera', 'Feodor', 'CPP5_s21_containers', 'Success', '11:46:00'::time);
call pcd_P2P_check('Xasan', 'Alexey', 'CPP5_s21_containers', 'Start', '11:46:00'::time);
call pcd_P2P_check('Xasan', 'Alexey', 'CPP5_s21_containers', 'Failure', '11:46:00'::time);
call pcd_P2P_check('Xasan', 'Alexey', 'CPP5_s21_containers', 'Success', '11:46:00'::time);

-- call pcd_Verter_check('Xasan', 'CPP5_s21_containers', 'Start', current_time::time);
-- call pcd_Verter_check('Xasan', 'CPP5_s21_containers', 'Success', current_time::time);

------------------------------------------------------------------------------------------------------------------------
--02
create or replace function fnc_verter_check_started(in pCheckedPeer varchar, in pTitleTask varchar) returns bool as
$$
begin
    return case
               when exists
                   (with tmp as (select max(v.id) as id, v.checkedpeer as cp, v.state
                                  from verter v
                                       left join p2p on v."Check" = p2p."Check"
                                       left join "checks" c on v."Check" = c.id
                                                                   and c.task = pTitleTask
                                  where c.peer = pCheckedPeer
                                  group by v.id
                                  order by 1 DESC
                                  limit 1)

                    select state
                    from tmp
                    where state = 'Start') then true
               else false
               end;
end;
$$ language plpgsql;

create or replace function fnc_p2p_check_success(in pCheckedPeer varchar, in pTitleTask varchar) returns bool as
$$
begin
    return case
               when exists
                   (with p3p as (select max(p2p.id) as id, p2p."Check", c.peer, c.task, p2p.state
                                  from "checks" c
                                        join p2p on c.id = p2p."Check"
                                                        and c.task = pTitleTask
                                  where c.peer = pCheckedPeer
                                  group by 2,3,4,5
                                  order by 1 desc
                                  limit 1)

                    select p3p.state
                    from p3p
                    where p3p.state = 'Success') then true
               else false
        end;
end;
$$ language plpgsql;

create or replace procedure pcd_Verter_check(in pCheckedPeer varchar,
                                             in pTitleTask varchar,
                                             in pState varchar,
                                             in "Time" time)
    LANGUAGE plpgsql as
$$
begin
    if pCheckedPeer in
       (select peer as CheckedPeer
        from p2p
                 inner join checks c on c.id = p2p."Check" and c.task = pTitleTask
        where p2p.state = 'Success'
         order by p2p."Time" desc)
         and
       ((select * from fnc_verter_check_started(CAST(pCheckedPeer as varchar), CAST(pTitleTask as varchar))) = true and
        pState <> 'Start') or
       (pState = 'Start' and
        (select * from fnc_verter_check_started(CAST(pCheckedPeer as varchar), CAST(pTitleTask as varchar))) = false
           and fnc_p2p_check_success(CAST(pCheckedPeer as varchar), CAST(pTitleTask as varchar)) = true)
    then
        insert into verter
        values ((SELECT MAX(ID) + 1 from verter),
                (select MAX(c.id) as id
                 from checks c
                          left join p2p on c.id = p2p."Check"
                 where peer = pCheckedPeer
                   and task = pTitleTask),
                pState,
                pCheckedPeer, "Time");
    end if;
end;
$$;

call pcd_Verter_check('Xasan', 'CPP5_s21_containers', 'Start', current_time::time);

call pcd_Verter_check('Valera', 'CPP5_s21_containers', 'Start', current_time::time);
call pcd_Verter_check('Valera', 'CPP5_s21_containers', 'Success', current_time::time);
call pcd_Verter_check('Feodor', 'CPP4_s21_matrix', 'Start', current_time::time);
call pcd_Verter_check('Feodor', 'CPP4_s21_matrix', 'Failure', current_time::time);
------------------------------------------------------------------------------------------------------------------------
--03
create or replace function fnc_trg_TransferredPoints()
    returns
        trigger
AS
$$
declare
    checked_peer text;
begin
    checked_peer := (select peer from checks where id = new."Check");
    if (TG_OP = 'INSERT') then
        if exists(select *
                  from transferredpoints
                  where checkingpeer = new.checkingpeer and checkedpeer = checked_peer) then
            update transferredpoints
            set pointsamount = pointsamount + 1
            where checkingpeer = NEW.checkingpeer
              and checkedpeer = (select peer
                                 from p2p
                                          inner join checks c on c.id = p2p."Check" and
                                                                 "Check" = NEW."Check" and
                                                                 state = 'Start'
                                 group by peer);
        else
            insert into transferredpoints
            values ((select max(id) + 1 from transferredpoints), new.checkingpeer, checked_peer, 1);
        end if;
        return NEW;
    end if;
end;
$$ language plpgsql;

create or replace trigger trg_TransferredPoints
after insert on p2p for each row
execute procedure fnc_trg_TransferredPoints();

call pcd_P2P_check('Xasan', 'Feodor', 'CPP5_s21_containers', 'Start', '11:46:00'::time);
------------------------------------------------------------------------------------------------------------------------
--04
create or replace function fnc_trg_XP() returns trigger AS
$$
begin
    if new.xpamount <= (select maxxp
                        from xp
                                 inner join checks c on c.id = xp."Check"
                                 inner join tasks t on t.title = c.task
                        group by maxxp) and
       new."Check" in (select "Check"
                       from verter
                       where state = 'Success') then
        return new;
    end if;
    return null;
end;
$$ language plpgsql;


create or replace trigger trg_XP
before insert on xp for each row
execute procedure fnc_trg_XP();

insert into xp
values (40, 1, 1000);
insert into xp
values (40, 5, 100);
