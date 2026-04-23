'use client'

import { useState, useEffect } from 'react'
import Image from 'next/image'
import { Play, Heart, Star, X, TrendingUp, Users, Eye, ThumbsUp, Film } from 'lucide-react'

interface Movie {
  id: string
  title: string
  genre: string
  thumbnail: string
  playbackId: string
  duration: number
  year: number
  rating: number
  likes: number
  views: number
}

const movies: Movie[] = [
  { id: 'A', title: 'Cosmic Journey', genre: 'Sci-Fi', thumbnail: 'https://images.unsplash.com/photo-1446776811953-b23d57bd21aa?w=800&q=80', playbackId: 'CosmicJourneyDemo', duration: 120, year: 2024, rating: 4.5, likes: 1250, views: 15000 },
  { id: 'B', title: 'The Last Stand', genre: 'Action', thumbnail: 'https://images.unsplash.com/photo-1536440136628-849c177e76a1?w=800&q=80', playbackId: 'TheLastStandDemo', duration: 95, year: 2023, rating: 4.2, likes: 980, views: 12000 },
  { id: 'C', title: 'Midnight Express', genre: 'Thriller', thumbnail: 'https://images.unsplash.com/photo-1478720568477-152d9b164e26?w=800&q=80', playbackId: 'MidnightExpressDemo', duration: 108, year: 2024, rating: 4.0, likes: 750, views: 9000 },
  { id: 'D', title: 'Love in Paris', genre: 'Romance', thumbnail: 'https://images.unsplash.com/photo-1518199266791-5375a83190b7?w=800&q=80', playbackId: 'LoveInParisDemo', duration: 115, year: 2023, rating: 4.3, likes: 2100, views: 18000 },
  { id: 'E', title: 'Haunted Manor', genre: 'Horror', thumbnail: 'https://images.unsplash.com/photo-1505635552108-c55f60a79460?w=800&q=80', playbackId: 'HauntedManorDemo', duration: 92, year: 2024, rating: 3.8, likes: 600, views: 7500 },
  { id: 'F', title: 'Comedy Night', genre: 'Comedy', thumbnail: 'https://images.unsplash.com/photo-1516280440614-6697288d5d38?w=800&q=80', playbackId: 'ComedyNightDemo', duration: 88, year: 2024, rating: 4.1, likes: 890, views: 11000 },
  { id: 'G', title: 'Ocean Deep', genre: 'Documentary', thumbnail: 'https://images.unsplash.com/photo-1582967788606-a171f1080ca8?w=800&q=80', playbackId: 'OceanDeepDemo', duration: 180, year: 2024, rating: 4.7, likes: 1500, views: 20000 },
  { id: 'H', title: 'Dragon Quest', genre: 'Fantasy', thumbnail: 'https://images.unsplash.com/photo-1519074069444-1ba4fff66d16?w=800&q=80', playbackId: 'DragonQuestDemo', duration: 142, year: 2023, rating: 4.4, likes: 3200, views: 25000 },
]

const genres = ['All', 'Sci-Fi', 'Action', 'Thriller', 'Romance', 'Horror', 'Comedy', 'Documentary', 'Fantasy']

const genreColors: Record<string, string> = {
  'Sci-Fi': '#00d2ff',
  'Action': '#ff4757',
  'Thriller': '#7d32a8',
  'Romance': '#ff6b9d',
  'Horror': '#8b0000',
  'Comedy': '#ffa502',
  'Documentary': '#2ed573',
  'Fantasy': '#5352ed',
  'All': '#808080'
}

interface ActivityEvent {
  user_id: string
  action: 'play' | 'like' | 'rate'
  movie_id: string
  genre: string
  timestamp: string
  rating_value?: number
}

interface TrendingGenre {
  genre: string
  score: number
}

interface MovieScore {
  movie_id: string
  genre: string
  score: number
  views: number
  likes: number
  avg_rating: number
}

function generateUserId(): string {
  if (typeof window === 'undefined') return 'user_dev'
  const stored = localStorage.getItem('netflix_user_id')
  if (stored) return stored
  const newId = 'user_' + Math.random().toString(36).substring(2, 10)
  localStorage.setItem('netflix_user_id', newId)
  return newId
}

const API_BASE = typeof window !== 'undefined' 
  ? (localStorage.getItem('api_base') || 'http://localhost:8000')
  : 'http://localhost:8000'

async function sendActivityEvent(event: ActivityEvent): Promise<void> {
  try {
    const res = await fetch(`${API_BASE}/api/activity`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event),
    })
    if (!res.ok) {
      console.error('Failed to send event:', res.status)
    }
  } catch (error) {
    console.error('Network error sending event:', error)
  }
}

export default function NetflixHome() {
  const [userId] = useState(() => generateUserId())
  const [selectedGenre, setSelectedGenre] = useState('All')
  const [selectedMovie, setSelectedMovie] = useState<Movie | null>(null)
  const [likedMovies, setLikedMovies] = useState<Set<string>>(new Set())
  const [movieRatings, setMovieRatings] = useState<Record<string, number>>({})
  const [scrolled, setScrolled] = useState(false)
  const [trendingGenres, setTrendingGenres] = useState<TrendingGenre[]>([])
  const [movieScores, setMovieScores] = useState<MovieScore[]>([])
  const [apiStatus, setApiStatus] = useState<'checking' | 'ok' | 'error'>('checking')
  const [totalEvents, setTotalEvents] = useState(0)
  const [filterByGenre, setFilterByGenre] = useState<string>('All')

  useEffect(() => {
    const handleScroll = () => setScrolled(window.scrollY > 50)
    window.addEventListener('scroll', handleScroll)
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  useEffect(() => {
    const checkApi = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/health`)
        if (res.ok) {
          const data = await res.json()
          setApiStatus('ok')
          setTotalEvents(data.events_processed || 0)
        } else {
          setApiStatus('error')
        }
      } catch {
        setApiStatus('error')
      }
    }
    checkApi()
    const interval = setInterval(checkApi, 10000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    const fetchTrending = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/trending`)
        if (res.ok) {
          const data = await res.json()
          setTrendingGenres(data.trending_genres || [])
          setMovieScores(data.movie_scores || [])
        }
      } catch (error) {
        console.error('Failed to fetch trending:', error)
      }
    }

    fetchTrending()
    const interval = setInterval(fetchTrending, 5000)
    return () => clearInterval(interval)
  }, [])

  const handlePlay = async (movie: Movie) => {
    setSelectedMovie(movie)
    await sendActivityEvent({
      user_id: userId,
      action: 'play',
      movie_id: movie.id,
      genre: movie.genre,
      timestamp: new Date().toISOString(),
    })
  }

  const handleLike = async (movie: Movie) => {
    const newLiked = new Set(likedMovies)
    const isLiked = newLiked.has(movie.id)
    
    if (isLiked) {
      newLiked.delete(movie.id)
    } else {
      newLiked.add(movie.id)
      await sendActivityEvent({
        user_id: userId,
        action: 'like',
        movie_id: movie.id,
        genre: movie.genre,
        timestamp: new Date().toISOString(),
      })
    }
    setLikedMovies(newLiked)
  }

  const handleRate = async (movie: Movie, rating: number) => {
    setMovieRatings(prev => ({ ...prev, [movie.id]: rating }))
    await sendActivityEvent({
      user_id: userId,
      action: 'rate',
      movie_id: movie.id,
      genre: movie.genre,
      timestamp: new Date().toISOString(),
      rating_value: rating,
    })
  }

  const filteredMovies = filterByGenre === 'All' 
    ? movies 
    : movies.filter(m => m.genre === filterByGenre)

  // Calculate stats
  const mostLikedMovie = movies.reduce((prev, current) => (prev.likes > current.likes) ? prev : current)
  const mostViewedMovie = movies.reduce((prev, current) => (prev.views > current.views) ? prev : current)
  const highestRatedMovie = movies.reduce((prev, current) => (prev.rating > current.rating) ? prev : current)
  const totalLikes = movies.reduce((sum, m) => sum + m.likes, 0)
  const totalViews = movies.reduce((sum, m) => sum + m.views, 0)

  // Real-time stats from API
  const realtimeMostLiked = movieScores.length > 0 
    ? movies.find(m => m.id === movieScores[0]?.movie_id) || mostLikedMovie
    : mostLikedMovie

  return (
    <>
      <nav className={`netflix-nav ${scrolled ? 'scrolled' : ''}`}>
        <span className="netflix-logo">NETFLIX</span>
        <div className="nav-links">
          <a href="#">Home</a>
          <a href="#">TV Shows</a>
          <a href="#">Movies</a>
          <a href="#">New & Popular</a>
        </div>
        <div className="nav-stats">
          <span className="stat-badge">
            <Eye size={14} /> {totalViews.toLocaleString()} views
          </span>
          <span className="stat-badge">
            <ThumbsUp size={14} /> {totalLikes.toLocaleString()} likes
          </span>
          <span className="api-status" style={{ color: apiStatus === 'ok' ? '#46d369' : '#f5222d' }}>
            {apiStatus === 'checking' ? 'Connecting...' : apiStatus === 'ok' ? '● Live' : '○ Offline'}
          </span>
        </div>
      </nav>

      <section className="hero">
        <div className="hero-bg" />
        <div className="hero-content">
          <span className="hero-badge">Featured</span>
          <h1 className="hero-title">Unlimited Entertainment</h1>
          <p className="hero-desc">Watch your favorite movies and track real-time engagement analytics with Kafka & Hadoop.</p>
          <div className="hero-stats">
            <span><Film size={16} /> {movies.length} Movies</span>
            <span><Users size={16} /> {genres.length - 1} Genres</span>
            <span><TrendingUp size={16} /> Real-time Analytics</span>
          </div>
          <div className="hero-buttons">
            <button className="btn btn-primary" onClick={() => handlePlay(movies[0])}>
              <Play size={20} /> Play Now
            </button>
            <button className="btn btn-secondary" onClick={() => alert(`${movies[0].title}: A thrilling ${movies[0].genre} movie from ${movies[0].year}. Runtime: ${movies[0].duration} minutes. Rating: ${movies[0].rating}/5`)}>
              More Info
            </button>
          </div>
        </div>
      </section>

      {/* Stats Overview Section */}
      <section className="stats-overview">
        <h2 className="section-title">Platform Statistics</h2>
        <div className="stats-grid">
          <div className="stat-card highlight">
            <div className="stat-icon" style={{ background: 'linear-gradient(135deg, #e50914, #ff6b6b)' }}>
              <Heart size={24} />
            </div>
            <div className="stat-content">
              <div className="stat-label">Most Liked</div>
              <div className="stat-value">{realtimeMostLiked?.title || mostLikedMovie.title}</div>
              <div className="stat-sublabel">{realtimeMostLiked?.likes.toLocaleString() || mostLikedMovie.likes.toLocaleString()} likes</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon" style={{ background: 'linear-gradient(135deg, #00d2ff, #3a7bd5)' }}>
              <Eye size={24} />
            </div>
            <div className="stat-content">
              <div className="stat-label">Most Viewed</div>
              <div className="stat-value">{mostViewedMovie.title}</div>
              <div className="stat-sublabel">{mostViewedMovie.views.toLocaleString()} views</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon" style={{ background: 'linear-gradient(135deg, #f9ca24, #f0932b)' }}>
              <Star size={24} />
            </div>
            <div className="stat-content">
              <div className="stat-label">Highest Rated</div>
              <div className="stat-value">{highestRatedMovie.title}</div>
              <div className="stat-sublabel">{highestRatedMovie.rating}/5 ⭐</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon" style={{ background: 'linear-gradient(135deg, #6c5ce7, #a29bfe)' }}>
              <TrendingUp size={24} />
            </div>
            <div className="stat-content">
              <div className="stat-label">Live Events</div>
              <div className="stat-value">{totalEvents}</div>
              <div className="stat-sublabel">processed via Kafka</div>
            </div>
          </div>
        </div>
      </section>

      {/* Trending Genres Section */}
      <section className="trending-section">
        <div className="section-header">
          <h2 className="section-title">
            <TrendingUp size={24} style={{ display: 'inline', marginRight: '8px', color: '#e50914' }} />
            Trending Genres (Last 5 Min)
          </h2>
          <span className="live-indicator">● Live Data</span>
        </div>
        <div className="trending-grid">
          {trendingGenres.length > 0 ? trendingGenres.slice(0, 8).map((item, idx) => (
            <div key={item.genre} className="trending-card" style={{ borderLeft: `4px solid ${genreColors[item.genre] || '#808080'}` }}>
              <div className="trending-rank">#{idx + 1}</div>
              <div className="trending-title">{item.genre}</div>
              <div className="trending-score">{item.score} engagements</div>
              <div className="trending-bar">
                <div className="trending-bar-fill" style={{ width: `${Math.min((item.score / Math.max(...trendingGenres.map(g => g.score))) * 100, 100)}%`, background: genreColors[item.genre] || '#808080' }} />
              </div>
            </div>
          )) : (
            <div className="trending-empty">
              <TrendingUp size={48} style={{ opacity: 0.3, marginBottom: '16px' }} />
              <div>No trending data yet. Start watching movies!</div>
            </div>
          )}
        </div>
      </section>

      {/* Genre Filter */}
      <section className="filter-section">
        <div className="genre-filters">
          {genres.map(genre => (
            <button
              key={genre}
              className={`genre-filter-btn ${filterByGenre === genre ? 'active' : ''}`}
              onClick={() => setFilterByGenre(genre)}
              style={{ 
                borderColor: filterByGenre === genre ? (genreColors[genre] || '#e50914') : 'transparent',
                background: filterByGenre === genre ? `${genreColors[genre]}20` : 'transparent'
              }}
            >
              {genre}
            </button>
          ))}
        </div>
      </section>

      {/* Movies Section */}
      <section className="content-section">
        <div className="section-header">
          <h2 className="section-title">
            {filterByGenre === 'All' ? 'Popular on Netflix' : `${filterByGenre} Movies`}
          </h2>
          <span className="movie-count">{filteredMovies.length} titles</span>
        </div>
        <div className="video-grid">
          {filteredMovies.map(movie => (
            <div key={movie.id} className="video-card" onClick={() => handlePlay(movie)}>
              <Image
                src={movie.thumbnail}
                alt={movie.title}
                fill
                sizes="(max-width: 768px) 50vw, 25vw"
                className="video-thumbnail"
              />
              <div className="genre-badge" style={{ background: genreColors[movie.genre] || '#808080' }}>
                {movie.genre}
              </div>
              <div className="video-overlay">
                <div className="video-info">
                  <button 
                    className="action-btn play-btn" 
                    onClick={(e) => { e.stopPropagation(); handlePlay(movie); }}
                    title="Play"
                  >
                    <Play size={18} fill="white" />
                  </button>
                  <button 
                    className={`action-btn ${likedMovies.has(movie.id) ? 'liked' : ''}`}
                    onClick={(e) => { e.stopPropagation(); handleLike(movie); }}
                    title={likedMovies.has(movie.id) ? 'Unlike' : 'Like'}
                  >
                    <Heart size={18} fill={likedMovies.has(movie.id) ? 'currentColor' : 'none'} />
                  </button>
                  <div className="rating-stars" onClick={(e) => e.stopPropagation()}>
                    {[1,2,3,4,5].map(star => (
                      <Star
                        key={star}
                        size={16}
                        className={`star ${star <= (movieRatings[movie.id] || 0) ? 'active' : ''}`}
                        fill={star <= (movieRatings[movie.id] || 0) ? '#ffd700' : 'none'}
                        onClick={() => handleRate(movie, star)}
                      />
                    ))}
                  </div>
                </div>
                <div className="video-stats">
                  <span><Eye size={12} /> {movie.views.toLocaleString()}</span>
                  <span><Heart size={12} /> {movie.likes.toLocaleString()}</span>
                  <span><Star size={12} /> {movie.rating}</span>
                </div>
                <div className="video-title">{movie.title}</div>
                <div className="video-meta">
                  <span>{movie.year}</span>
                  <span>{movie.duration} min</span>
                  <span className="quality-badge">HD</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Real-time Leaderboard */}
      {movieScores.length > 0 && (
        <section className="leaderboard-section">
          <h2 className="section-title">🏆 Live Leaderboard</h2>
          <div className="leaderboard-grid">
            {movieScores.slice(0, 5).map((score, idx) => {
              const movie = movies.find(m => m.id === score.movie_id)
              if (!movie) return null
              return (
                <div key={score.movie_id} className="leaderboard-item">
                  <div className="leaderboard-rank">{idx + 1}</div>
                  <div className="leaderboard-movie">
                    <div className="leaderboard-title">{movie.title}</div>
                    <div className="leaderboard-genre" style={{ color: genreColors[movie.genre] }}>{movie.genre}</div>
                  </div>
                  <div className="leaderboard-score">
                    <div className="score-value">{Math.round(score.score)} pts</div>
                    <div className="score-breakdown">
                      {score.views} views • {score.likes} likes
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </section>
      )}

      {/* Tech Stack Info */}
      <section className="tech-section">
        <h2 className="section-title">Powered By</h2>
        <div className="tech-grid">
          <div className="tech-item">
            <div className="tech-name">Apache Kafka</div>
            <div className="tech-desc">Real-time event streaming</div>
          </div>
          <div className="tech-item">
            <div className="tech-name">Hadoop HDFS</div>
            <div className="tech-desc">Distributed data storage</div>
          </div>
          <div className="tech-item">
            <div className="tech-name">FastAPI</div>
            <div className="tech-desc">High-performance backend</div>
          </div>
          <div className="tech-item">
            <div className="tech-name">Next.js</div>
            <div className="tech-desc">React frontend framework</div>
          </div>
        </div>
      </section>

      {selectedMovie && (
        <div className="modal-overlay" onClick={() => setSelectedMovie(null)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <div className="modal-header-left">
                <span className="modal-genre-badge" style={{ background: genreColors[selectedMovie.genre] }}>
                  {selectedMovie.genre}
                </span>
                <h3 className="modal-title">{selectedMovie.title}</h3>
              </div>
              <button className="close-btn" onClick={() => setSelectedMovie(null)}>
                <X size={24} />
              </button>
            </div>
            <div className="modal-stats">
              <span><Eye size={14} /> {selectedMovie.views.toLocaleString()} views</span>
              <span><Heart size={14} /> {selectedMovie.likes.toLocaleString()} likes</span>
              <span><Star size={14} /> {selectedMovie.rating}/5</span>
              <span>{selectedMovie.year}</span>
              <span>{selectedMovie.duration} min</span>
            </div>
            <div className="player-container">
              <video
                controls
                autoPlay
                style={{ height: '100%', width: '100%', objectFit: 'contain' }}
                poster={selectedMovie.thumbnail}
              >
                <source 
                  src="https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4" 
                  type="video/mp4" 
                />
                Your browser does not support the video tag.
              </video>
            </div>
            <div className="player-overlay">
              Now Playing: {selectedMovie.title} ({selectedMovie.genre})
            </div>
          </div>
        </div>
      )}

      <footer className="footer">
        <div className="footer-grid">
          <div>
            <a href="#">Audio Description</a>
            <a href="#">Help Center</a>
            <a href="#">Gift Cards</a>
          </div>
          <div>
            <a href="#">Media Center</a>
            <a href="#">Investor Relations</a>
            <a href="#">Jobs</a>
          </div>
          <div>
            <a href="#">Terms of Service</a>
            <a href="#">Privacy</a>
            <a href="#">Cookie Preferences</a>
          </div>
        </div>
        <button className="service-code">Service Code</button>
      </footer>
    </>
  )
}
