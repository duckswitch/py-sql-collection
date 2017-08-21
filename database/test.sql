

SELECT
*
FROM
(
    SELECT
    task.id AS 'id',
    task.name AS 'name',

    task.project_id AS 'project_id.id',
    `project_id`.name AS 'project_id.name',

    `project_id`.project_manager_user_id AS 'project_id.project_manager_user_id.id',
    `project_id.project_manager_user_id`.name AS 'project_id.project_manager_user_id.name',

    `project_id`.developer_user_id AS 'project_id.developer_user_id.id',
    `project_id.developer_user_id`.name AS 'project_id.developer_user_id.name',

    `project_id`.id AS 'project_id.client_id.id',
    `project_id.client_id`.name AS '`.name',

    `project_id.client_id`.country_id AS 'project_id.client_id.country_id.id',
    `project_id.client_id.country_id`.name AS 'project_id.client_id.country_id.name',

    `task`.affected_user_id AS 'affected_user_id.id',
    `affected_user_id`.name AS 'affected_user_id.name'
    FROM
    task `task`

    LEFT JOIN project `project_id` --
    ON `task`.project_id = `project_id`.id

    LEFT JOIN user `project_id.project_manager_user_id` --
    ON `project_id`.project_manager_user_id = `project_id.project_manager_user_id`.id

    LEFT JOIN user `project_id.developer_user_id` --
    ON `project_id`.developer_user_id = `project_id.developer_user_id`.id

    LEFT JOIN client `project_id.client_id` --
    ON `project_id`.client_id = `project_id.client_id`.id

    LEFT JOIN country `project_id.client_id.country_id`
    ON `project_id.client_id`.country_id = `project_id.client_id.country_id`.id

    LEFT JOIN user `affected_user_id` --
    ON `task`.affected_user_id = `affected_user_id`.id
) AS A0
WHERE `affected_user_id.id` = 1
;

