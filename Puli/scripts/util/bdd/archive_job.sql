# Identify all foldernodes hierarchy (on main level)
#select TOP.id as TOPid,TOP.name as TOPname, 
#	FN.id as FNid,FN.parent_id as FNparent,FN.name as FNname, FN.archived as FNarch 
#from folder_nodes as FN 
#inner join folder_nodes as TOP on FN.parent_id = TOP.id ;

# Identify all foldernodes hierarchy (on 2 levels: graph and main level)
#select TOP.id as TOPid,TOP.name as TOPname, 
#	L1.id as L1id,L1.parent_id as L1parent,L1.name as L1name, L1.archived as L1arch,
#	FN.id as FNid,FN.parent_id as FNparent,FN.name as FNname, FN.archived as FNarch 
#from folder_nodes as FN 
#inner join folder_nodes as L1 on FN.parent_id = L1.id
#inner join folder_nodes as TOP on L1.parent_id = TOP.id ;

# --> FOLDER NODES to remove: (169,172)

#### TASK NODES / FOLDER NODES
#select id,name,task_id,archived  from task_nodes where parent_id in (169,172); # liste de tasks
#select id,name,task_group_id,archived  from folder_nodes where id in (169,172); # liste de taskgroups

update task_nodes set archived=1 where id in 
	( select id from 
		(
			select id from task_nodes where parent_id in (177) 
		) as TMP_task_nodes
	);

update folder_nodes set archived=1 where id in (177);

#### TASKS / TASK GROUPS
#select task_id from task_nodes where parent_id in (174); # liste de tasks
#select task_group_id from folder_nodes where id in (174); # liste de taskgroups

update task_groups set archived=1 where id in 
	( select task_group_id from folder_nodes where id in (177) );

update tasks set archived=1 where id in
	( select task_id from task_nodes where parent_id in (177) );

#### COMMANDS
#select task_id from task_nodes where parent_id in (174);
select id, archived from commands where task_id in
	( select task_id from task_nodes where parent_id in (177) );

update commands set archived=1 where id in 
	( select id from 
		(
			select id from commands where task_id in ( select task_id from task_nodes where parent_id in (177) )
		) as TMP_command
	);

#### POOL_SHARES
#select * from pool_shares where node_id in (177);
update pool_shares set archived = 1 where node_id in (177);
