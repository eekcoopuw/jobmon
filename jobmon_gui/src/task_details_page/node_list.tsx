import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Link } from "react-router-dom";


export default function NodeLists({ upstreamTasks, downstreamTasks }) {
    return (
        <div>
            <h2>Dependencies</h2>
            <div className="card-columns d-flex justify-content-center">
                <div className="card dependency-list-scroll">
                    <div className="card-header">Upstream Task IDs</div>
                    <ul className="list-group list-group-flush">
                        {
                            upstreamTasks.flat(1).map(d => (
                                <li className="list-group-item">
                                    <Link
                                        to={{ pathname: `/task_details/${d["id"]}` }}
                                        key={d["id"]}>{d["id"]}
                                    </Link>
                                </li>
                            ))
                        }
                    </ul>
                </div>
                <div className="card dependency-list-scroll">
                    <div className="card-header">Downstream Task IDs</div>
                    <ul className="list-group list-group-flush">
                        {
                            downstreamTasks.flat(1).map(d => (
                                <li className="list-group-item">
                                    <Link
                                        to={{ pathname: `/task_details/${d["id"]}` }}
                                        key={d["id"]}>{d["id"]}
                                    </Link>
                                </li>
                            ))
                        }
                    </ul>
                </div>
            </div>
        </div>
    );
}