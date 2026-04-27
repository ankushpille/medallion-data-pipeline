import { useCallback, useEffect, useRef, useState } from 'react';
import ReactFlow, { Background } from 'reactflow';

const FLOW_FIT_OPTIONS = { padding: 0.3, minZoom: 0.1, duration: 350 };

export default function PipelineFlowCanvas({ nodes, edges, nodeTypes, onNodeClick, isStreaming = false }) {
  const [flowInstance, setFlowInstance] = useState(null);
  const containerRef = useRef(null);
  const stableNodeTypesRef = useRef(nodeTypes);
  const stableNodeTypes = stableNodeTypesRef.current;
  const fitTimeoutsRef = useRef([]);

  const clearScheduledFits = useCallback(() => {
    fitTimeoutsRef.current.forEach((id) => clearTimeout(id));
    fitTimeoutsRef.current = [];
  }, []);

  const safeFitView = useCallback(() => {
    if (!flowInstance || !nodes?.length) return;
    try {
      // Multi-pass fit helps when layout is still settling during animated/streamed updates.
      clearScheduledFits();
      const fitDelays = [0, 70, 160, 320, 520];
      fitDelays.forEach((delay) => {
        const id = setTimeout(() => {
          requestAnimationFrame(() => {
            flowInstance.fitView(FLOW_FIT_OPTIONS);
          });
        }, delay);
        fitTimeoutsRef.current.push(id);
      });
    } catch {
      // no-op: avoid hard crash from transient viewport states
    }
  }, [flowInstance, nodes, clearScheduledFits]);

  useEffect(() => {
    safeFitView();
  }, [safeFitView, nodes, edges]);

  useEffect(() => {
    if (!flowInstance || !isStreaming) return;
    const id = setInterval(() => {
      safeFitView();
    }, 650);
    return () => clearInterval(id);
  }, [flowInstance, isStreaming, safeFitView]);

  useEffect(() => {
    if (!containerRef.current || !flowInstance) return;
    const ro = new ResizeObserver(() => {
      safeFitView();
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [flowInstance, safeFitView]);

  useEffect(() => {
    return () => {
      clearScheduledFits();
    };
  }, [clearScheduledFits]);

  return (
    <div
      style={{
        height: 450,
        background: '#fff',
        border: '1px solid var(--border)',
        borderTop: 'none',
        borderBottomLeftRadius: 16,
        borderBottomRightRadius: 16,
        position: 'relative',
        overflowX: 'auto',
        overflowY: 'hidden'
      }}
    >
      <div
        ref={containerRef}
        style={{
          minWidth: 1320,
          width: '100%',
          height: '100%'
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={stableNodeTypes}
          onInit={(instance) => {
            setFlowInstance(instance);
            requestAnimationFrame(() => {
              instance.fitView(FLOW_FIT_OPTIONS);
              setTimeout(() => instance.fitView(FLOW_FIT_OPTIONS), 100);
            });
          }}
          onNodeClick={onNodeClick}
          minZoom={0.1}
          fitViewOptions={{ padding: 0.3, minZoom: 0.1 }}
          zoomOnScroll={false}
          zoomOnPinch={false}
          zoomOnDoubleClick={false}
          panOnDrag={false}
          panOnScroll={false}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          selectionOnDrag={false}
          selectNodesOnDrag={false}
          nodesFocusable={false}
          edgesFocusable={false}
        >
          <Background color="#f1f5f9" gap={20} />
        </ReactFlow>
      </div>
    </div>
  );
}
