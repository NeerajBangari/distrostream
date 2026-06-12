import React, { useEffect, useState } from 'react';
import { PlayCircleOutlined, SyncOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';

interface VideoMetadata {
  video_id: any;
  status: 'PROCESSING' | 'COMPLETED' | 'FAILED';
  playback_url: string | null;
  created_at: string;
}

interface VideoHistoryPanelProps {
  onSelectVideo: (playbackUrl: string, videoId: string) => void;
  activeVideoId?: string;
  refreshTrigger: number; // Increment this to force a re-fetch after a new upload
}

export const VideoHistoryPanel: React.FC<VideoHistoryPanelProps> = ({ 
  onSelectVideo, 
  activeVideoId,
  refreshTrigger 
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
      console.error("Failed to fetch video library history:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchVideoHistory();
  }, [refreshTrigger]);

  const renderStatusBadge = (status: VideoMetadata['status']) => {
    switch (status) {
      case 'COMPLETED':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-950 text-green-400 border border-green-800">
            <CheckCircleOutlined /> Ready
          </span>
        );
      case 'PROCESSING':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-blue-950 text-blue-400 border border-blue-800 animate-pulse">
            <SyncOutlined spin /> Transcoding
          </span>
        );
      case 'FAILED':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-red-950 text-red-400 border border-red-800">
            <CloseCircleOutlined /> Failed
          </span>
        );
    }
  };

  return (
    <div className="bg-[#111827] border border-gray-800 rounded-xl p-4 flex flex-col h-full min-h-[400px]">
      <div className="flex items-center justify-between pb-3 border-b border-gray-800 mb-3">
        <h3 className="text-md font-semibold text-gray-200 tracking-wide">Media Library History</h3>
        <span className="text-xs text-gray-400 font-mono">{videos.length} items</span>
      </div>

      {loading ? (
        <div className="flex-1 flex items-center justify-center text-gray-400 gap-2 text-sm font-mono">
          <SyncOutlined spin className="text-emerald-500" /> Querying PostgreSQL...
        </div>
      ) : videos.length === 0 ? (
        <div className="flex-1 flex items-center justify-center text-gray-500 text-sm italic py-8">
          No records found in database.
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto space-y-2 pr-1 custom-scrollbar max-h-[500px]">
          {videos.map((video) => {
            const isActive = activeVideoId === video.video_id;
            const isReady = video.status === 'COMPLETED' && video.playback_url;

            return (
              <div
                key={video.video_id}
                onClick={() => isReady && onSelectVideo(video.playback_url!, video.video_id)}
                className={`w-full text-left p-3 rounded-lg border transition-all flex flex-col gap-2 ${
                  isReady 
                    ? 'cursor-pointer hover:border-emerald-500/50 hover:bg-gray-800/40' 
                    : 'cursor-not-allowed opacity-60 bg-gray-900/20'
                } ${
                  isActive 
                    ? 'bg-emerald-950/30 border-emerald-500/80 shadow-[0_0_12px_rgba(16,185,129,0.1)]' 
                    : 'bg-gray-900/50 border-gray-800'
                }`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-2 min-w-0">
                    <PlayCircleOutlined className={`text-lg flex-shrink-0 ${isActive ? 'text-emerald-400' : 'text-gray-400'}`} />
                    <span className="text-sm font-medium text-gray-200 truncate font-mono">
                      {video.video_id}
                    </span>
                  </div>
                </div>

                <div className="flex items-center justify-between text-xs text-gray-400 pt-1 border-t border-gray-800/60 mt-1">
                  {renderStatusBadge(video.status)}
                  <span className="font-mono text-[11px]">
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