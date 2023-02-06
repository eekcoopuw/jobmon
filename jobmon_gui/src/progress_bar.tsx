import React from 'react';
import ProgressBar from 'react-bootstrap/ProgressBar';
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';

import './jobmon_gui.css';

export default function JobmonProgressBar({tasks, pending, scheduled, running, done, fatal, num_attempts_avg, num_attempts_min, num_attempts_max, maxc, placement, style="striped"}) {
    num_attempts_avg = num_attempts_avg.toFixed(1);
    // style can be striped or animated; others will be treated as default
    if (style === "striped") {
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <table>
                                    <tr>
                                    <td>
                                    foo
                                    </td>
                                    <td>
                                    bar
                                    </td>
                                    </tr>
                                    </table>
                                    FOO
                                    Total: {tasks};
                                    Pending: {pending};
                                    Scheduled: {scheduled};
                                    <br />
                                    Running: {running};
                                    Done: {done};
                                    Fatal: {fatal};
                                    <br />
                                    # Attempts: {num_attempts_avg} ({num_attempts_min} - {num_attempts_max})
                                    <br /><br />
                                    Concurrency Limit: {maxc}
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" striped max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" striped max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" striped max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" striped max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" striped max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }else if(style === "animated" ){
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    Total: {tasks};
                                    Pending: {pending};
                                    Scheduled: {scheduled};
                                    <br />
                                    Running: {running};
                                    Done: {done};
                                    Fatal: {fatal};
                                    <br />
                                    # Attempts: {num_attempts_avg} ({num_attempts_min} - {num_attempts_max})
                                    <br /><br />
                                    Concurrency Limit: {maxc}
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" animated max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" animated max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" animated max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" animated max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" animated max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }else{
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task-count">
                                    <table id="tt-stats">
                                        <tr>
                                            <th> Total:</th>
                                            <td>{tasks}</td>
                                            <td className="tt-stats-spacer">&nbsp;</td>
                                            <th> Pending:</th>
                                            <td>{pending}</td>
                                        </tr>
                                        <tr>
                                            <th> Scheduled:</th>
                                            <td>{scheduled}</td>
                                            <td className="tt-stats-spacer">&nbsp;</td>
                                            <th> Running:</th>
                                            <td>{running}</td>
                                        </tr>
                                        <tr>
                                            <th> Done:</th>
                                            <td>{done}</td>
                                            <td className="tt-stats-spacer">&nbsp;</td>
                                            <th> Fatal:</th>
                                            <td>{fatal}</td>
                                        </tr>
                                        <tr>
                                            <td colSpan={5}><hr/></td>
                                        </tr>
                                        <tr>
                                            <th colSpan={3}># Attempts:</th>
                                            <td colSpan={2}>{num_attempts_avg} ({num_attempts_min} - {num_attempts_max})</td>
                                        </tr>
                                        <tr>
                                            <th colSpan={3}>Concurrency Limit:</th>
                                            <td colSpan={2}>{maxc}</td>
                                        </tr>
                                    </table>
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }
}
