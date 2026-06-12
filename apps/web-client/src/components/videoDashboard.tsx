import React, { useEffect, useRef, useState } from 'react';
import Hls from 'hls.js';
import { VideoHistoryPanel } from './videoHistory'; // 👈 Import the sidebar component

interface TranscodingData {
  video_id: string;
  status: string;
  playback_url: string;
}

interface NotificationEvent {
  event: 'VIDEO_TRANSCODING_COMPLETE';
  data: TranscodingData;
}

export const VideoDashboard: React.FC = () => {
  const [playbackUrl, setPlaybackUrl] = useState<string>('');
  const [currentVideoId, setCurrentVideoId] = useState<string>('');
  const [logs, setLogs] = useState<string[]>([]);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'connected' | 'disconnected'>('connecting');
  
  // 👇 Shared reactive trigger token to sync historical records across layers
  const [refreshHistoryToken, setRefreshHistoryToken] = useState<number>(0);
  
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const hlsRef = useRef<Hls | null>(null);

  // Core Gateway Subscription Engine
  useEffect(() => {
    const socketUrl = 'ws://127.0.0.1:8080/api/v1/notifications/connect';
    const ws = new WebSocket(socketUrl);

    ws.onopen = () => {
      setWsStatus('connected');
      addLog('🔌 Connected to local live notification gateway (Port 8080)');
    };

    ws.onmessage = (event) => {
      try {
        const payload: NotificationEvent = JSON.parse(event.data);
        if (payload.event === 'VIDEO_TRANSCODING_COMPLETE') {
          addLog(`🚨 Event Caught: Video [${payload.data.video_id}] is ready for streaming!`);
          
          // 1. Hot swap the primary video player target engine context
          setCurrentVideoId(payload.data.video_id);
          setPlaybackUrl(payload.data.playback_url);
          
          // 2. 🚀 Trigger immediate silent history refresh to catch the 'Ready' state change
          setRefreshHistoryToken(prev => prev + 1);
        }
      } catch (err) {
        addLog('📥 Received raw socket message stream.');
      }
    };

    ws.onclose = () => {
      setWsStatus('disconnected');
      addLog('❌ Socket disconnected from server.');
    };

    return () => {
      ws.close();
    };
  }, []);

  // HLS Stream Playback Orchestrator
  useEffect(() => {
    if (!playbackUrl || !videoRef.current) return;

    if (hlsRef.current) {
      hlsRef.current.destroy();
    }

    const videoElement = videoRef.current;

    if (Hls.isSupported()) {
      const hls = new Hls({ maxBufferLength: 10 });
      hlsRef.current = hls;
      hls.loadSource(playbackUrl);
      hls.attachMedia(videoElement);
      
      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        addLog(`🎬 HLS Manifest parsed successfully. Beginning stream engine...`);
        videoElement.play().catch(() => addLog('⚠️ Autoplay blocked by browser. Interaction required.'));
      });

      hls.on(Hls.Events.ERROR, (_, data) => {
        if (data.fatal) {
          addLog(`❌ HLS Engine Error: ${data.type} - Attempting recovery...`);
          hls.recoverMediaError();
        }
      });
    } 
    else if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
      videoElement.src = playbackUrl;
      videoElement.addEventListener('loadedmetadata', () => {
        videoElement.play();
      });
    }
  }, [playbackUrl]);

  const addLog = (message: string) => {
    setLogs((prev) => [`[${new Date().toLocaleTimeString()}] ${message}`, ...prev]);
  };

  // 👇 Interactive click callback logic triggered by history items
  const handleSelectVideoFromLibrary = (url: string, id: string) => {
    addLog(`🎯 Library item selected: Loading stream for context key [${id}]`);
    setCurrentVideoId(id);
    setPlaybackUrl(url);
  };

  return (
    <div className="min-h-screen bg-[#0f0f15] text-[#f1f1f7] p-8 font-sans">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Header App Identity Context Banner */}
        <header className="flex justify-between items-center bg-[#161622] p-5 border border-[#232336] rounded-xl shadow-lg">
          <div>
            <h1 className="text-2xl font-bold tracking-tight bg-gradient-to-r from-[#00adb5] to-[#4caf50] bg-clip-text text-transparent">
              DistroStream Control Panel
            </h1>
            <p className="text-sm text-gray-400 mt-1">Distributed Event-Driven Video Architecture</p>
          </div>
          <div className="flex items-center space-x-2">
            <span className={`h-3 w-3 rounded-full ${
              wsStatus === 'connected' ? 'bg-emerald-500 animate-pulse' : wsStatus === 'connecting' ? 'bg-amber-500' : 'bg-rose-500'
            }`} />
            <span className="text-sm font-medium uppercase tracking-wider text-gray-300">
              Gateway: {wsStatus}
            </span>
          </div>
        </header>

        {/* Updated Grid System Container Layout */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          
          {/* Main Monitor Display Area */}
          <div className="lg:col-span-2 bg-[#161622] border border-[#232336] rounded-xl p-4 flex flex-col justify-between shadow-md">
            <div>
              <h2 className="text-lg font-semibold text-white mb-3">
                {currentVideoId ? `Now Playing: ${currentVideoId}` : 'Awaiting Video Pipeline Target...'}
              </h2>
              <div className="relative aspect-video w-full bg-black rounded-lg overflow-hidden border border-gray-800">
                {playbackUrl ? (
                  <video 
                    ref={videoRef} 
                    controls 
                    className="w-full h-full object-contain"
                  />
                ) : (
                  <div className="absolute inset-0 flex flex-col items-center justify-center text-center p-6 space-y-3">
                    <div className="text-4xl animate-bounce">📺</div>
                    <p className="text-gray-400 max-w-sm">
                      Upload a file through Swagger. Once the transcoder worker outputs HLS chunks, the screen will switch on automatically.
                    </p>
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

          {/* Right Column Layout Group: Media Library & Log Output Panels */}
          <div className="lg:col-span-2 grid grid-cols-1 md:grid-cols-2 gap-6 lg:flex lg:flex-col lg:space-y-6">
            
            {/* 💾 MEDIA LIBRARY PANEL COMPONENT */}
            <div className="flex-1">
              <VideoHistoryPanel 
                onSelectVideo={handleSelectVideoFromLibrary}
                activeVideoId={currentVideoId}
                refreshTrigger={refreshHistoryToken}
              />
            </div>

            {/* SYSTEM EVENT LOG PANEL */}
            <div className="flex-1 bg-[#161622] border border-[#232336] rounded-xl p-4 flex flex-col h-[320px] lg:h-[240px] shadow-md">
              <h3 className="text-md font-semibold text-white mb-3 flex items-center justify-between">
                <span>System Event Logs</span>
                <button 
                  onClick={() => setLogs([])}
                  className="text-xs text-gray-500 hover:text-[#00adb5] transition-colors"
                >
                  Clear
                </button>
              </h3>
              <div className="flex-1 bg-[#09090e] border border-gray-800 rounded-lg p-3 overflow-y-auto font-mono text-xs space-y-2 flex flex-col-reverse">
                {logs.length === 0 ? (
                  <p className="text-gray-600 text-center italic my-auto">No events registered yet...</p>
                ) : (
                  logs.map((log, index) => (
                    <div 
                      key={index} 
                      className={`leading-relaxed py-1 border-b border-[#141420] last:border-0 ${
                        log.includes('🚨') ? 'text-[#00adb5] font-semibold' : log.includes('❌') ? 'text-rose-400' : 'text-gray-400'
                      }`}
                    >
                      {log}
                    </div>
                  ))
                )}
              </div>
            </div>

          </div> {/* End Right Column Layout Group */}

        </div> {/* End Grid */}
      </div>
    </div>
  );
};