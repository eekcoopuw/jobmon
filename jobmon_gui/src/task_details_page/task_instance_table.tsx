import React from 'react';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import BootstrapTable, { ColumnDescription } from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";


export default function TaskInstanceTable({ taskInstanceData }) {

    const columns: Array<ColumnDescription> = [
        {
            dataField: "ti_id",
            text: "ID",
            sort: true,
        },
        {
            dataField: "ti_status",
            text: "Status",
            sort: true,
        },
        {
            dataField: "ti_stdout",
            text: "Stdout Path",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_stderr",
            text: "Stderr Path",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_distributor_id",
            text: "Distributor ID",
            sort: true,
        },
        {
            dataField: "ti_nodename",
            text: "Node Name",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_error_log_description",
            text: "Error Log",
            sort: true,
            style: { overflowWrap: 'break-word' },
        }
    ]

    // Create and return the React Bootstrap Table
    return (
        <div>
            <h2>Task Instances</h2>
            <BootstrapTable
                keyField="ti_id"
                data={taskInstanceData}
                columns={columns}
                bootstrap4
                headerClasses="thead-dark"
                striped
                pagination={taskInstanceData.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
            />
        </div>
    );
}