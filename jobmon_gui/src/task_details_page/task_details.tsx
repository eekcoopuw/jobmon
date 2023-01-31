import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useParams } from 'react-router-dom';
import TaskInstanceTable from './task_instance_table';
import NodeLists from './node_list';
import TaskFSM from './task_fsm';

function getTaskStatus(setTaskStatus, taskId) {
    const url = process.env.REACT_APP_BASE_URL + "/task/get_task_status_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        setTaskStatus(result.data.task_status)
    };
    return fetchData
}

function getTaskDetail(setTaskDetails, taskId) {
    const url = process.env.REACT_APP_BASE_URL + "/task/get_task_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        let tis = result.data.taskinstances;
        setTaskDetails(tis)
    };
    return fetchData
}

function getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId) {
    const url = process.env.REACT_APP_BASE_URL + "/task_dependencies/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        let data = result.data;
        setUpstreamTasks(data["up"])
        setDownstreamTasks(data["down"])
    };
    return fetchData
}

function TaskDetails() {
    let params = useParams();
    let taskId = params.taskId
    const [task_details, setTaskDetails] = useState([])
    const [upstream_tasks, setUpstreamTasks] = useState([])
    const [downtream_tasks, setDownstreamTasks] = useState([])
    const [task_status, setTaskStatus] = useState([])


    //***********************hooks******************************
    useEffect(() => {
        getTaskDetail(setTaskDetails, taskId)();
        getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId)();
        getTaskStatus(setTaskStatus, taskId)();
    }, [taskId]);

    return (
        <div>
            <div>
                <header className="App-header">
                    <p>Task ID: {taskId}</p>
                </header>
            </div>
            <div className="div-level-2">
                <TaskFSM taskStatus={task_status}/>
            </div>
            <div className="div-level-2">
                <NodeLists upstreamTasks={upstream_tasks} downstreamTasks={downtream_tasks}/>
            </div>
            <div id="wftable" className="div-level-2" >
                <TaskInstanceTable taskInstanceData={task_details} />
            </div>
        </div>

    );

}

export default TaskDetails;