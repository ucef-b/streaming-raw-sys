import { useEffect, useRef } from "react";

export function useImageStream(onFrame) {
  const wsRef = useRef(null);

  useEffect(() => {
    // Generate the protocol (ws or wss).
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    // Verify that this URL will match the one from the server.
    const wsUrl = `${protocol}//${window.location.hostname}:8000/ws/images`;
    console.log(`Attempting to connect to WebSocket at ${wsUrl}`);
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      console.log('WebSocket connected');
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    ws.onmessage = (evt) => {
      const msg = JSON.parse(evt.data);
      const img = new Image();
      img.src = "data:image/jpeg;base64," + msg.jpeg;
      img.onload = () => onFrame(img, msg.metadata);
    };

    wsRef.current = ws;
    return () => ws.close();
  }, [onFrame]);
}