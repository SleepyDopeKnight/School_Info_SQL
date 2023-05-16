create table Peers
(
    Nickname varchar not null primary key,
    Birthday date    not null
);

insert into Peers
values ('Feodor', '1999-06-05');
insert into Peers
values ('Valera', '2001-12-02');
insert into Peers
values ('Danila', '2003-01-01');
insert into Peers
values ('Xasan', '1997-06-16');
insert into Peers
values ('Alexey', '1963-05-19');

create table Tasks
(
    Title      varchar not null primary key,
    ParentTask varchar,
    MaxXP      integer,

    constraint fk_Tasks_ParentTask foreign key (ParentTask) references Tasks (Title)
);

insert into Tasks
values ('CPP4_s21_matrix', NULL, 600);
insert into Tasks
values ('CPP5_s21_containers', 'CPP4_s21_matrix', 700);
insert into Tasks
values ('CPP6_s21_calc', 'CPP5_s21_containers', 800);
insert into Tasks
values ('A4_s21_mem', 'CPP6_s21_calc', 900);
insert into Tasks
values ('A5_s21_memory', 'A4_s21_mem', 1000);

create table Friends
(
    id    bigint primary key,
    Peer1 varchar not null,
    Peer2 varchar not null,

    constraint fk_Friends_Peer1 foreign key (Peer1) references Peers (Nickname),
    constraint fk_Friends_Peer2 foreign key (Peer2) references Peers (Nickname)
);

insert into Friends
values (1, 'Feodor', 'Valera');
insert into Friends
values (2, 'Feodor', 'Danila');
insert into Friends
values (3, 'Danila', 'Valera');
insert into Friends
values (4, 'Xasan', 'Danila');
insert into Friends
values (5, 'Xasan', 'Feodor');

create table TransferredPoints
(
    id           bigint primary key,
    CheckingPeer varchar not null,
    CheckedPeer  varchar not null,
    PointsAmount integer default (0),

    constraint fk_friends_CheckingPeer foreign key (CheckingPeer) references Peers (Nickname),
    constraint fk_friends_CheckedPeer foreign key (CheckedPeer) references Peers (Nickname)
);

insert into TransferredPoints
values (1, 'Valera', 'Feodor', 3);
insert into TransferredPoints
values (2, 'Feodor', 'Valera', 1);
insert into TransferredPoints
values (3, 'Xasan', 'Danila', 1);
insert into TransferredPoints
values (4, 'Danila', 'Xasan', 1);
insert into TransferredPoints
values (5, 'Feodor', 'Alexey', 2);
insert into TransferredPoints
values (6, 'Alexey', 'Feodor', 1);
insert into TransferredPoints
values (7, 'Valera', 'Danila', 1);

create table Checks
(
    id   bigint primary key,
    Peer varchar not null,
    Task varchar not null,
    Date date    not null,

    constraint fk_Checks_Peer foreign key (Peer) references Peers (Nickname),
    constraint fk_Checks_Task foreign key (Task) references Tasks (Title)
);

insert into Checks
values (1, 'Feodor', 'CPP4_s21_matrix', '2022-05-03');
insert into Checks
values (2, 'Valera', 'CPP4_s21_matrix', '2022-05-03');
insert into Checks
values (3, 'Danila', 'CPP4_s21_matrix', '2022-05-10');
insert into Checks
values (4, 'Xasan', 'CPP4_s21_matrix', '2022-05-14');
insert into Checks
values (5, 'Alexey', 'CPP4_s21_matrix', '2022-05-19');
insert into Checks
values (6, 'Alexey', 'CPP4_s21_matrix', '2022-05-24');
insert into Checks
values (7, 'Danila', 'CPP5_s21_containers', '2022-06-05');
insert into Checks
values (8, 'Feodor', 'CPP5_s21_containers', '2022-06-05');
insert into Checks
values (9, 'Feodor', 'CPP4_s21_matrix', '2022-06-06');

create table XP
(
    id       bigint primary key,
    "Check"  bigint not null,
    XPAmount integer,

    constraint fk_XP_Check foreign key ("Check") references Checks (id)
);

insert into XP
values (1, 1, 600);
insert into XP
values (2, 4, 587);
insert into XP
values (3, 6, 591);
insert into XP
values (4, 7, 666);
insert into XP
values (5, 8, 672);
insert into XP
values (6, 9, 590);

create table P2P
(
    id           bigint primary key,
    "Check"      bigint                      not null,
    CheckingPeer varchar                     not null,
    State        varchar                     not null,
    "Time"       time not null,

    constraint fk_P2P_Check foreign key ("Check") references Checks (id),
    constraint fk_P2P_CheckingPeer foreign key (CheckingPeer) references Peers (Nickname)
);

alter table P2P
    add constraint ch_State check (State in ('Start', 'Failure', 'Success'));


insert
into P2P
values (1, 1, 'Valera', 'Start', '15:46');
insert into P2P
values (2, 1, 'Valera', 'Success', '16:05');
insert into P2P
values (3, 2, 'Feodor', 'Start', '13:23');
insert into P2P
values (4, 2, 'Feodor', 'Failure', '13:30');
insert into P2P
values (5, 3, 'Xasan', 'Start', '14:48');
insert into P2P
values (6, 3, 'Xasan', 'Success', '15:17');
insert into P2P
values (7, 4, 'Danila', 'Start', '13:37');
insert into P2P
values (8, 4, 'Danila', 'Success', '14:01');
insert into P2P
values (9, 5, 'Feodor', 'Start', '12:28');
insert into P2P
values (10, 5, 'Feodor', 'Success', '12:54');
insert into P2P
values (11, 6, 'Feodor', 'Start', '02:28');
insert into P2P
values (12, 6, 'Feodor', 'Success', '3:00');
insert into P2P
values (13, 7, 'Alexey', 'Start', '4:20');
insert into P2P
values (14, 7, 'Alexey', 'Success', '4:43');
insert into P2P
values (15, 8, 'Valera', 'Start', '8:15:');
insert into P2P
values (16, 8, 'Valera', 'Success', '9:00');
insert into P2P
values (17, 9, 'Valera', 'Start', '4:20');
insert into P2P
values (18, 9, 'Valera', 'Success', '4:29');

create table Verter
(
    id           bigint primary key,
    "Check"      bigint                      not null,
    State        varchar                     not null,
    CheckedPeer varchar                     not null,
    "Time"       time not null,

    constraint fk_verter_Check foreign key ("Check") references Checks (id)
);

insert into Verter
values (1, 1, 'Start', 'Feodor', '16:05:00');
insert into Verter
values (2, 1, 'Success', 'Feodor', '16:09:00');
insert into Verter
values (3, 3, 'Start', 'Danila', '15:17:00');
insert into Verter
values (4, 3, 'Failure', 'Danila', '15:22:00');
insert into Verter
values (5, 4, 'Start', 'Xasan', '14:01:00');
insert into Verter
values (6, 4, 'Success', 'Xasan', '14:07:00');
insert into Verter
values (7, 5, 'Start', 'Alexey', '12:54:00');
insert into Verter
values (8, 5, 'Failure', 'Alexey', '13:03:00');
insert into Verter
values (9, 6, 'Start', 'Alexey', '3:00:00');
insert into Verter
values (10, 6, 'Success', 'Alexey', '4:02:00');
insert into Verter
values (11, 9, 'Start', 'Feodor', '15:00:00');
insert into Verter
values (12, 9, 'Success', 'Feodor', '15:02:00');

create table Recommendations
(
    id              bigint primary key,
    Peer            varchar not null,
    RecommendedPeer varchar not null,

    constraint fr_Recommendations_Peer foreign key (Peer) references Peers (Nickname),
    constraint fr_Recommendations_RecommendedPeer foreign key (RecommendedPeer) references Peers (Nickname)
);

insert into Recommendations
values (1, 'Feodor', 'Valera');
insert into Recommendations
values (2, 'Valera', 'Feodor');
insert into Recommendations
values (3, 'Feodor', 'Danila');
insert into Recommendations
values (4, 'Danila', 'Xasan');
insert into Recommendations
values (5, 'Alexey', 'Xasan');
insert into Recommendations
values (6, 'Danila', 'Valera');
insert into Recommendations
values (7, 'Valera', 'Xasan');

create table TimeTracking
(
    id    bigint primary key,
    Peer  varchar not null,
    Date  date    not null,
    Time  time    not null,
    State integer not null,

    constraint fk_TimeTracking_Peer foreign key (Peer) references Peers (Nickname)
);

insert into TimeTracking
values (1, 'Feodor', '2022-12-01', '11:27', '1');
insert into TimeTracking
values (2, 'Danila', '2022-12-01', '13:41', '1');
insert into TimeTracking
values (3, 'Feodor', '2022-12-01', '15:48', '2');
insert into TimeTracking
values (4, 'Feodor', '2022-12-01', '16:13', '1');
insert into TimeTracking
values (5, 'Danila', '2022-12-01', '17:12', '2');
insert into TimeTracking
values (6, 'Xasan', '2022-12-01', '18:16', '1');
insert into TimeTracking
values (7, 'Danila', '2022-12-01', '18:36', '1');
insert into TimeTracking
values (8, 'Feodor', '2022-12-01', '21:18', '2');
insert into TimeTracking
values (9, 'Danila', '2022-12-01', '22:22', '2');
insert into TimeTracking
values (10, 'Feodor', '2022-12-01', '22:29', '1');
insert into TimeTracking
values (11, 'Xasan', '2022-12-01', '23:21', '2');
insert into TimeTracking
values (12, 'Feodor', '2022-12-01', '23:59', '2');


CREATE OR REPLACE PROCEDURE export(IN tablename text, IN path text, IN Separator CHAR) AS $$
BEGIN
EXECUTE format('COPY %s TO %L WITH CSV HEADER DELIMITER ''%s'';', $1, $2, $3 );
END;
$$ LANGUAGE PLPGSQL;

set import_export_path.txt to '/Users/chamomiv/SQL2_Info21_v1.0-0/src/import_export/';

call export('checks', (current_setting('import_export_path.txt') || 'export_checks.csv'), '|');
call export('friends', (current_setting('import_export_path.txt') || 'export_friends.csv'), '|');
call export('p2p', (current_setting('import_export_path.txt') || 'export_p2p.csv'), '|');
call export('peers', (current_setting('import_export_path.txt') || 'export_peers.csv'), '|');
call export('recommendations', (current_setting('import_export_path.txt') || 'export_recommendations.csv'), '|');
call export('tasks', (current_setting('import_export_path.txt') || 'export_tasks.csv'), '|');
call export('timetracking', (current_setting('import_export_path.txt') || 'export_timetracking.csv'), '|');
call export('transferredpoints', (current_setting('import_export_path.txt') || 'export_transferredpoints.csv'), '|');
call export('verter', (current_setting('import_export_path.txt') || 'export_verter.csv'), '|');
call export('xp', (current_setting('import_export_path.txt') || 'export_xp.csv'), '|');

CREATE OR REPLACE PROCEDURE import(IN tablename text, IN path text, IN Separator CHAR) AS $$
BEGIN
EXECUTE format('COPY %s FROM %L WITH CSV HEADER DELIMITER ''%s'';', $1, $2, $3 );
END;
$$ LANGUAGE PLPGSQL;

call import('checks', (current_setting('import_export_path.txt') || 'export_checks.csv'), '|');
call import('friends', (current_setting('import_export_path.txt') || 'export_friends.csv'), '|');
call import('p2p', (current_setting('import_export_path.txt') || 'export_p2p.csv'), '|');
call import('peers', (current_setting('import_export_path.txt') || 'export_peers.csv'), '|');
call import('recommendations', (current_setting('import_export_path.txt') || 'export_recommendations.csv'), '|');
call import('tasks', (current_setting('import_export_path.txt') || 'export_tasks.csv'), '|');
call import('timetracking', (current_setting('import_export_path.txt') || 'export_timetracking.csv'), '|');
call import('transferredpoints', (current_setting('import_export_path.txt') || 'export_transferredpoints.csv'), '|');
call import('verter', (current_setting('import_export_path.txt') || 'export_verter.csv'), '|');
call import('xp', (current_setting('import_export_path.txt') || 'export_xp.csv'), '|');

----------------------------------- additional

create or replace function fnc_success_checks()
    returns table
            (
                check_id     bigint,
                CheckedPeer  varchar,
                CheckingPeer varchar,
                State        varchar,
                Task         varchar,
                "Time"       date
            )
as
$$
with succes_check_id as ((select "Check"
                          from p2p)
                         except
                         (select "Check"
                          from p2p
                          where State = 'Failure')
                         except
                         (select "Check"
                          from verter
                          where state = 'Failure'))

select C.id, Peer as CheckedPeer, p2p.CheckingPeer, State, Task, date
from succes_check_id
         inner join p2p on P2P."Check" = succes_check_id."Check"
         inner join Checks C on C.id = succes_check_id."Check"
$$ language sql;