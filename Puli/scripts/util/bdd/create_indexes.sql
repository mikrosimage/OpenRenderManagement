select * from tasks where id=237644;
select * from task_groups;
select * from task_groups where id=237644;
select * from folder_nodes where task_group_id=252935;

# INVALID JOBS TO DELETE
# 237638, 239880, 252929, 263997, 264001, 285513, 285619, 285643


#DONE
create index IDX_commands__task_id on commands (task_id);
create index IDX_commands__archived on commands (archived);
create index IDX_pool_shares__pool_id on pool_shares (pool_id);
create index IDX_pool_shares__node_id on pool_shares (node_id);
create index IDX_pool_shares__archived on pool_shares (archived);
create index IDX_tasks__parent_id on tasks(parent_id);
create index IDX_tasks__archived on tasks(archived);
create index IDX_task_groups__parent_id on task_groups(parent_id);
create index IDX_task_groups__archived on task_groups(archived);
create index IDX_task_nodes__parent_id on task_nodes(parent_id);
create index IDX_task_nodes__task_id on task_nodes(task_id);
create index IDX_task_nodes__creation_time on task_nodes(creation_time);
create index IDX_task_nodes__end_time on task_nodes(end_time);
create index IDX_task_nodes__archived on task_nodes(archived);
create index IDX_folder_nodes__parent_id on folder_nodes(parent_id);
create index IDX_folder_nodes__task_group_id on folder_nodes(task_group_id);
create index IDX_folder_nodes__archived on folder_nodes(archived);
