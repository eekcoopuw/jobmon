import React from "react";
import ReactFlow, {
    Background,
    Controls,
    MarkerType,
    MiniMap,
    Node,
    Position
} from 'reactflow';

import 'reactflow/dist/style.css';

function TaskFSM({ taskStatus }) {
    const task_status = taskStatus[0]
    const nodes: Node[] = [
        {
            id: 'register',
            data: { label: 'G: Registering' },
            type: 'input',
            position: { x: 100, y: 150 },
            sourcePosition: Position.Right,
            ...(task_status === "G" ? {style: { backgroundColor: '#26AADF' }} : null)
        },
        {
            id: 'queued',
            data: { label: 'Q: Queued' },
            position: { x: 275, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "Q" ? {style: { backgroundColor: '#26AADF' }} : null)
        },
        {
            id: 'instantiate',
            data: { label: 'I: Instantiating' },
            position: { x: 450, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "I" ? {style: { backgroundColor: '#26AADF' }} : null)
        },
        {
            id: 'launched',
            data: { label: 'O: Launched' },
            position: { x: 625, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "O" ? {style: { backgroundColor: '#26AADF' }} : null)
        },
        {
            id: 'running',
            data: { label: 'R: Running' },
            position: { x: 800, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "R" ? {style: { backgroundColor: '#26AADF' }} : null)
        },
        {
            id: 'done',
            data: { label: 'D: Done' },
            position: { x: 1000, y: 200 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "D" ? {style: { backgroundColor: '#04CA22' }} : null)
        },
        {
            id: 'recoverable',
            data: { label: 'E: Error Recoverable' },
            position: { x: 1000, y: 100 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "E" ? {style: { backgroundColor: '#FFC300' }} : null)
        },
        {
            id: 'fatal',
            data: { label: 'F: Error Fatal ' },
            position: { x: 1200, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "F" ? {style: { backgroundColor: '#ED3333' }} : null)
        },
        {
            id: 'adjusting',
            data: { label: 'A: Adjusting Resources' },
            position: { x: 1200, y: 50 },
            sourcePosition: Position.Top,
            targetPosition: Position.Left,
            ...(task_status === "A" ? {style: { backgroundColor: '#FFC300' }} : null)
        },
    ];
    
    const edges = [
        {
            id: 'register-queued',
            source: 'register',
            target: 'queued',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'queued-instantiate',
            source: 'queued',
            target: 'instantiate',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'instantiate-launched',
            source: 'instantiate',
            target: 'launched',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'launched-running',
            source: 'launched',
            target: 'running',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'running-done',
            source: 'running',
            target: 'done',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'running-recoverable',
            source: 'running',
            target: 'recoverable',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'recoverable-fatal',
            source: 'recoverable',
            target: 'fatal',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'recoverable-adjusting',
            source: 'recoverable',
            target: 'adjusting',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'adjusting-queued',
            source: 'adjusting',
            target: 'queued',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
    
    ];
    return (
        <div>
            <h2>Task Finite State Machine</h2>
            <div style={{ height: 400 }}>
                <ReactFlow nodes={nodes} edges={edges}>
                    <Background />
                    <Controls />
                    <MiniMap zoomable pannable />
                </ReactFlow>
            </div>
        </div>
    );
}

export default TaskFSM;