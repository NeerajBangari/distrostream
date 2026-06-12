import React, { useEffect, useRef, useState } from 'react';
import Hls from 'hls.js';
import { VideoHistoryPanel } from './videoHistory';

interface TranscodingData {
  video_id: string;
  status: string;
  playback_url: string;
}

interface ProgressData {
  video_id: string;
  progress: number;
}

// Discriminator Union Types for our WebSocket message receiver
type SocketPayload = 
  | { event: 'VIDEO_TRANSCODING_COMPLETE'; data: TranscodingData }
  | { event: 'VIDEO_PROGRESS_UPDATE'; data: ProgressData };

export const VideoDashboard: React.FC = () => {
  const [playbackUrl, setPlaybackUrl] = useState<string>('');
  const [currentVideoId, setCurrentVideoId] = useState<string>('');
  const [logs, setLogs] = useState<string[]>([]);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'connected' | 'disconnected'>('connecting');
  const [refreshHistoryToken, setRefreshHistoryToken] = useState<number>(0);
  
  // 👇 Cache container maps video IDs to their live processing percentage frames
  const [progressMap, setProgressMap] = useState<Record<string, number>>({});
  
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const hlsRef = useRef<Hls | null>(null);

  useEffect(() => {
    const socketUrl = 'ws://127.0.0.1:8080/api/v1/notifications/connect';
    const ws = new WebSocket(socketUrl);

    ws.onopen = () => {
      setWsStatus('connected');
      addLog('🔌 Connected to local live notification gateway (Port 8080)');
    };

    ws.onmessage = (event) => {
      try {
        const payload: SocketPayload = JSON.parse(event.data);
        
        // 🛑 BRANCH A: Live Percentage Updates
        if (payload.event === 'VIDEO_PROGRESS_UPDATE') {
          setProgressMap(prev => ({
            ...prev,
            [payload.data.video_id]: payload.data.progress
          }));
        }
        
        // 🏁 BRANCH B: Pipeline Complete Updates
        else if (payload.event === 'VIDEO_TRANSCODING_COMPLETE') {
          addLog(`🚨 Pipeline Complete: Video [${payload.data.video_id}] is ready!`);
          setCurrentVideoId(payload.data.video_id);
          setPlaybackUrl(payload.data.playback_url);
          setRefreshHistoryToken(prev => prev + 1); // Refresh catalog from Postgres
        }
      } catch (err) {
        // Fallback for non-JSON message payloads
      }
    };

    ws.onclose = () => {
      setWsStatus('disconnected');
      addLog('❌ Socket disconnected from server.');
    };

    return () => {
      if (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, []);

  // HLS playback engine controller logic loop
  useEffect(() => {
    if (!playbackUrl || !videoRef.current) return;
    if (hlsRef.current) hlsRef.current.destroy();

    const videoElement = videoRef.current;
    if (Hls.isSupported()) {
      const hls = new Hls({ maxBufferLength: 10 });
      hlsRef.current = hls;
      hls.loadSource(playbackUrl);
      hls.attachMedia(videoElement);
      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        videoElement.play().catch(() => addLog('⚠️ Autoplay blocked by browser. Interaction required.'));
      });
    } else if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
      videoElement.src = playbackUrl;
      videoElement.addEventListener('loadedmetadata', () => videoElement.play());
    }
  }, [playbackUrl]);

  const addLog = (message: string) => {
    setLogs((prev) => [`[${new Date().toLocaleTimeString()}] ${message}`, ...prev]);
  };

  const handleSelectVideoFromLibrary = (url: string, id: string) => {
    setCurrentVideoId(id);
    setPlaybackUrl(url);
  };

  return (
    <div className="min-h-screen bg-[#0f0f15] text-[#f1f1f7] p-8 font-sans">
      <div className="max-w-7xl mx-auto space-y-6">
        
        <header className="flex justify-between items-center bg-[#161622] p-5 border border-[#232336] rounded-xl shadow-lg">
          <div>
            <h1 className="text-2xl font-bold tracking-tight bg-gradient-to-r from-[#00adb5] to-[#4caf50] bg-clip-text text-transparent">
              DistroStream Control Panel
            </h1>
            <p className="text-sm text-gray-400 mt-1">Distributed Event-Driven Video Architecture</p>
          </div>
          <div className="flex items-center space-x-2">
            <span className={`h-3 w-3 rounded-full ${wsStatus === 'connected' ? 'bg-emerald-500 animate-pulse' : wsStatus === 'connecting' ? 'bg-amber-500' : 'bg-rose-500'}`} />
            <span className="text-sm font-medium uppercase tracking-wider text-gray-300">Gateway: {wsStatus}</span>
          </div>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <div className="lg:col-span-2 bg-[#161622] border border-[#232336] rounded-xl p-4 flex flex-col justify-between shadow-md">
            <div>
              <h2 className="text-lg font-semibold text-white mb-3">
                {currentVideoId ? `Now Playing: ${currentVideoId}` : 'Awaiting Video Pipeline Target...'}
              </h2>
              <div className="relative aspect-video w-full bg-black rounded-lg overflow-hidden border border-gray-800">
                {playbackUrl ? (
                  <video ref={videoRef} controls className="w-full h-full object-contain" />
                ) : (
                  <div className="absolute inset-0 flex flex-col items-center justify-center text-center p-6 space-y-3">
                    <div className="text-4xl animate-bounce">📺</div>
                    <p className="text-gray-400 max-w-sm">Upload a video to see real-time transcoding percentages and automated playback engine execution.</p>
                  </div>
                )}
              </div>
            </div>
            {playbackUrl && (
              <div className="mt-4 p-3 bg-[#1e1e2f] border border-gray-800 rounded-lg text-xs truncate select-all">
                <span className="text-[#00adb5] font-semibold">Manifest Path:</span> {playbackUrl}
              </div>
            )}
          </div>

          <div className="lg:col-span-2 grid grid-cols-1 md:grid-cols-2 gap-6 lg:flex lg:flex-col lg:space-y-6">
            <div className="flex-1">
              {/* Injecting our tracking progress map cache directly into our layout component */}
              <VideoHistoryPanel 
                onSelectVideo={handleSelectVideoFromLibrary}
                activeVideoId={currentVideoId}
                refreshTrigger={refreshHistoryToken}
                activeProgressMap={progressMap} 
              />
            </div>

            <div className="flex-1 bg-[#161622] border border-[#232336] rounded-xl p-4 flex flex-col h-[240px] shadow-md">
              <h3 className="text-md font-semibold text-white mb-3 flex items-center justify-between">
                <span>System Event Logs</span>
                <button onClick={() => setLogs([])} className="text-xs text-gray-500 hover:text-[#00adb5]">Clear</button>
              </h3>
              <div className="flex-1 bg-[#09090e] border border-gray-800 rounded-lg p-3 overflow-y-auto font-mono text-xs space-y-2 flex flex-col-reverse">
                {logs.length === 0 ? (
                  <p className="text-gray-600 text-center italic my-auto">No events registered yet...</p>
                ) : (
                  logs.map((log, index) => (
                    <div key={index} className={`leading-relaxed py-1 border-b border-[#141420] last:border-0 ${log.includes('🚨') ? 'text-[#00adb5] font-semibold' : 'text-gray-400'}`}>
                      {log}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

        </div> 
      </div>
    </div>
  );
};