import React, { useEffect, useState } from 'react';
import { PlayCircleOutlined, SyncOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';

interface VideoMetadata {
  video_id: string;
  status: 'PROCESSING' | 'COMPLETED' | 'FAILED';
  playback_url: string | null;
  created_at: string;
}

interface VideoHistoryPanelProps {
    onSelectVideo: (playbackUrl: string, videoId: string) => void;
    activeVideoId?: string;
    refreshTrigger: number;
    activeProgressMap?: Record<string, number>; // 👈 Added '?' to make it optional
  }

export const VideoHistoryPanel: React.FC<VideoHistoryPanelProps> = ({ 
  onSelectVideo, 
  activeVideoId,
  refreshTrigger,
  activeProgressMap
}) => {
  const [videos, setVideos] = useState<VideoMetadata[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  const fetchVideoHistory = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8000/api/v1/videos');
      if (response.ok) {
        const data = await response.json();
        setVideos(data);
      }
    } catch (error) {
      console.error("Failed to query database catalog architecture:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchVideoHistory();
  }, [refreshTrigger]);

  return (
    <div className="bg-[#161622] border border-[#232336] rounded-xl p-4 flex flex-col h-full shadow-md">
      <div className="flex items-center justify-between pb-3 border-b border-gray-800 mb-3">
        <h3 className="text-md font-semibold text-gray-200 tracking-wide">Media Library History</h3>
        <span className="text-xs text-gray-400 font-mono">{videos.length} items</span>
      </div>

      {loading ? (
        <div className="flex-1 flex items-center justify-center text-gray-400 gap-2 text-sm font-mono py-8">
          <SyncOutlined spin className="text-[#00adb5]" /> Querying PostgreSQL...
        </div>
      ) : videos.length === 0 ? (
        <div className="flex-1 flex items-center justify-center text-gray-500 text-sm italic py-8">
          No records found in library database.
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto space-y-2 max-h-[460px] pr-1">
          {videos.map((video) => {
            const isActive = activeVideoId === video.video_id;
            const isReady = video.status === 'COMPLETED' && video.playback_url;
            
            // 👇 Extract any active live process percentages from our root context tracking object map
            const liveProgress = (activeProgressMap || {})[video.video_id] ?? (video.status === 'PROCESSING' ? 0 : 100);

            return (
              <div
                key={video.video_id}
                onClick={() => isReady && onSelectVideo(video.playback_url!, video.video_id)}
                className={`w-full text-left p-3 rounded-lg border transition-all flex flex-col gap-2 ${
                  isReady 
                    ? 'cursor-pointer hover:border-[#00adb5]/50 hover:bg-gray-800/20' 
                    : 'cursor-not-allowed opacity-80 bg-gray-900/40'
                } ${
                  isActive 
                    ? 'bg-[#1e1e2f] border-[#00adb5] shadow-lg' 
                    : 'bg-[#0e0e15] border-gray-800'
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2 min-w-0">
                    <PlayCircleOutlined className={`text-md ${isActive ? 'text-[#00adb5]' : 'text-gray-400'}`} />
                    <span className="text-sm font-medium text-gray-200 truncate font-mono">
                      {video.video_id}
                    </span>
                  </div>
                </div>

                {/* 📊 LIVE PROGRESS BAR ROUTINE ELEMENT LAYOUT */}
                {video.status === 'PROCESSING' && liveProgress < 100 && (
                  <div className="w-full mt-1">
                    <div className="flex justify-between text-[11px] font-mono text-gray-400 mb-1">
                      <span className="flex items-center gap-1"><SyncOutlined spin className="text-amber-500" /> Transcoding...</span>
                      <span>{liveProgress}%</span>
                    </div>
                    <div className="w-full bg-gray-800 rounded-full h-1.5 overflow-hidden">
                      <div 
                        className="bg-gradient-to-r from-amber-500 to-[#00adb5] h-1.5 rounded-full transition-all duration-300 ease-out"
                        style={{ width: `${liveProgress}%` }}
                      />
                    </div>
                  </div>
                )}

                <div className="flex items-center justify-between text-xs text-gray-400 pt-1 border-t border-gray-800/40 mt-1">
                  <div>
                    {video.status === 'COMPLETED' && (
                      <span className="inline-flex items-center gap-1 text-emerald-400 font-medium text-[11px]">
                        <CheckCircleOutlined /> Ready
                      </span>
                    )}
                    {video.status === 'FAILED' && (
                      <span className="inline-flex items-center gap-1 text-rose-400 font-medium text-[11px]">
                        <CloseCircleOutlined /> Failed
                      </span>
                    )}
                  </div>
                  <span className="font-mono text-[10px]">
                    {new Date(video.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};