import React, { useState } from 'react';
import { VideoHistoryPanel } from './components/videoHistory';
import { VideoDashboard } from '../src/components/videoDashboard';

export const App: React.FC = () => {
  const [activePlaybackUrl, setActivePlaybackUrl] = useState<string>('');
  const [activeVideoId, setActiveVideoId] = useState<string>('');
  const [refreshHistoryToken, setRefreshHistoryToken] = useState<number>(0);

  const handleVideoSelectFromLibrary = (url: string, id: string) => {
    setActivePlaybackUrl(url);
    setActiveVideoId(id);
    console.log(`Now streaming source content key: [${id}]`);
  };

  const handleUploadSuccessNotification = () => {
    // 💡 Whenever a video finishes uploading successfully via your upload panel hook,
    // increment this token to force the Media Library view to immediately refresh from Postgres!
    setRefreshHistoryToken(prev => prev + 1);
  };

  return (
    <div className="min-h-screen bg-[#0B0F19] text-white p-6">
      {/* Head Header Banner element */}
      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* LEFT & CENTER PANEL: Your existing Live Streaming Player & Event Log elements */}
        <div className="lg:col-span-2 space-y-6">
          <div className="bg-[#111827] border border-gray-800 rounded-xl p-4">
            {/* Pass activePlaybackUrl to your existing HLS stream engine engine element */}
            <h2 className="text-lg font-bold mb-3 font-mono text-emerald-400">
              Now Playing: {activeVideoId || "Select a stream..."}
            </h2>
            {activePlaybackUrl ? (
              <video src={activePlaybackUrl} controls className="w-full rounded-lg" />
            ) : (
              <div className="aspect-video bg-gray-900 rounded-lg flex items-center justify-center text-gray-500 italic">
                No stream running. Choose a processed video from your library history.
              </div>
            )}
          </div>
          
          <VideoDashboard />
        </div>

        {/* RIGHT PANEL: Your Live PostgreSQL History Engine */}
        <div className="lg:col-span-1">
          <VideoHistoryPanel 
            onSelectVideo={handleVideoSelectFromLibrary}
            activeVideoId={activeVideoId}
            refreshTrigger={refreshHistoryToken}
          />
        </div>

      </div>
    </div>
  );
};