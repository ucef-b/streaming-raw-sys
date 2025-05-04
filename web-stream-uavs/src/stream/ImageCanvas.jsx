import React, { useRef } from "react";
import { useImageStream } from "./useImageStream";

export default function ImageCanvas() {
  const canvasRef = useRef();
  
  useImageStream((img, metadata) => {
    const ctx = canvasRef.current.getContext("2d");
    canvasRef.current.width = img.width;
    canvasRef.current.height = img.height;
    ctx.drawImage(img, 0, 0);
  });

  return (<>
  <canvas ref={canvasRef} style={{ border: "1px solid #ccc" }} />
  </>)
}
