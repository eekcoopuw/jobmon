import React, { useEffect, useState } from 'react';
import moment from 'moment';
import BootstrapTable from "react-bootstrap-table-next";
import filterFactory, { textFilter } from 'react-bootstrap-table2-filter';
import { sanitize } from 'dompurify';

import '../jobmon_gui.css';
import { convertDate } from '../functions'

export default function Errors({ errorLogs, tt_name, loading, apm }) {
    const s: any = apm.startSpan("errors", "custom");

    const [errorDetail, setErrorDetail] = useState({'error': '', 'error_time': '', 'task_id': '',
          'task_instance_err_id': '', 'task_instance_id': '', 'time_since': ''});
    const [helper, setHelper] = useState("");

    function getTimeSince(date: string) {
        let date_obj = convertDate(date);
        const dateTimeAgo = moment(date_obj).fromNow();
        return dateTimeAgo;
    }

    //error trim
    function error_trim(s) {
        var length = 40;
        if (s.length > length){
           var trimmed_s = s.substring(0, length) + "...";
           return trimmed_s;
        }else{
           return s;
        }
    }

    function get_error_brief(errors) {
        var r: any = [];

        for (var i in errors) {
            var e = errors[i];
            var cell_display = "<div><p style='color:grey;text-align:right;font-size:8px'>".concat(getTimeSince(e.error_time)).concat("</p><p>").concat(error_trim(e.error)).concat("</p></div>");
            r.push({"id": i, "brief": cell_display});
        }
        return r;
    }

    const htmlFormatter = cell => {
        // add sanitize to prevent xss attack
        return <div dangerouslySetInnerHTML={{ __html: sanitize(`${cell}`) }} />;
    };

    const error_brief = get_error_brief(errorLogs);
    const columns = [
        {
            dataField: "id",
            text: "Error Index",
            hidden: true
        },

        {
            dataField: "brief",
            text: "",
            filter: textFilter(),
            formatter: htmlFormatter,
            headerEvents: {
                onMouseEnter: (e, column, columnIndex) => {
                    setHelper("The list of task instance error logs with filter. Click to view the error detail.");
                },
                onMouseLeave: (e, column, columnIndex) => {
                    setHelper("");
                }
            },
            events: {
                onMouseEnter: (e, column, columnIndex, row, rowIndex) => {
                    setHelper("Task Instance ID: ".concat(errorLogs[rowIndex].task_instance_id));
                },
                onMouseLeave: (e, column, columnIndex, row, rowIndex) => {
                    setHelper("");
                }
            }
        }
    ];

    //label click event
    function error_clicked(d) {
        let e = {'error': '', 'error_time': '', 'task_id': '', 'task_instance_err_id': '', 'task_instance_id': '', 'time_since': ''};
        e.task_id = d.task_id;
        e.task_instance_id = d.task_instance_id;
        e.task_instance_err_id = d.task_instance_err_id;
        e.error = d.error;

        // calculate time from now
        let error_date_obj = convertDate(d.error_time);
        e.error_time = error_date_obj.toLocaleString("en-US", { timeZone: "America/Los_Angeles", dateStyle: "full", timeStyle: "long" });
        e.time_since = getTimeSince(d.error_time);

        setErrorDetail(e);
    }

    const tableRowEvents = {
       onClick: (e, row, rowIndex) => {
         setErrorDetail(errorLogs[rowIndex]);
       },
    }

    //hook
    useEffect(() => {
        // clean the error log detail display (right side) when task template changes
        let temp = {'error': '', 'error_time': '', 'task_id': '',
          'task_instance_err_id': '', 'task_instance_id': '', 'time_since': ''};
        setErrorDetail(temp);
    }, [errorLogs]);

    // logic: when task template name selected, show a loading spinner; when loading finished and there is no error, show a no error message; when loading finished and there are errors, show error logs
    return(
        <div>
            <div className="div_floatleft">
                <span className="span-helper"><i>{helper}</i></span>
                <br/>
                {errorLogs.length !== 0 && loading === false &&
                  <BootstrapTable
                                keyField="id"
                                bootstrap4
                                headerClasses="thead-dark"
                                striped
                                data={ error_brief }
                                columns={ columns }
                                filter={ filterFactory() }
                                rowEvents={ tableRowEvents }
                                selectRow={{
                                    mode: "radio",
                                    hideSelectColumn: true,
                                    clickToSelect: true,
                                    bgColor: "#848884",
                                  }}

                            />
                }

            </div>

            <div className="div_floatright_align_left">
                 {errorDetail.task_id !== "" && loading === false &&
                     <div className="div-error">

                             Task ID: {errorDetail.task_id}
                             <br/>
                             Task Instance ID: {errorDetail.task_instance_id}
                             <br/>
                             Error Time: {errorDetail.error_time}
                             <br/>
                             <br/>
                             Error Log:
                             <div className="div-break-word">
                                 {errorDetail.error}
                             </div>

                     </div>
                 }

            </div>

            {errorLogs.length === 0 && tt_name !== "" && loading === false &&
                    <div>
                        <br/>
                        There is no error log associated with task template <i>{tt_name}</i>.
                    </div>
            }

            {tt_name !== "" && loading &&
                    <div>
                        <br/>
                        <div className="loader" />
                    </div>
            }
        </div>
    )
    s.end();
}