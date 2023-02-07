import React, { useState } from 'react';
import { faSearch } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import TaskTable from './task_table';

export default function Tasks({ tasks, onSubmit, register, loading }) {
    return (
        <div>
            <br></br>
            <div className="div_floatleft">
                <form onSubmit={onSubmit}>
                    <label className="label-left">TaskTemplate Name:&nbsp;&nbsp;  </label>
                    <input id="task_template_Name" type="text" {...register("task_template_name")} required />
                    <label className="label-right"><FontAwesomeIcon icon={faSearch} onClick={onSubmit} /> </label>
                </form>
            </div>
            <TaskTable taskData={tasks} loading={loading} />
        </div>
    )

}