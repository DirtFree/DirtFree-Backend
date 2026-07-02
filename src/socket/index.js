import { Server } from 'socket.io';
import logger from '../config/logger.js';
import { auth, db } from '../config/firebase.js';
import redis from '../config/redis.js';
import { findNearestPartners, normalizeSpecialization } from '../services/matchmaking.js';

export const initSocket = (server) => {
  const io = new Server(server, {
    cors: {
      origin: "*", // Adjust this in production to your domain
      methods: ["GET", "POST"]
    },
    pingTimeout: 60000,
  });

  const loadPartnerServices = async (partnerId, socket) => {
    if (!db || !partnerId) return [];
    try {
      if (socket && socket.selectedServices) {
        return socket.selectedServices;
      }
      const doc = await db.collection('partners').doc(partnerId).get();
      if (doc.exists) {
        const services = doc.data().selectedServices || [];
        if (socket) {
          socket.selectedServices = services;
        }
        await redis.hset(`partner:${partnerId}`, 'selectedServices', JSON.stringify(services));
        return services;
      }
    } catch (err) {
      logger.warn(`⚠️ Failed to load services for partner ${partnerId}: ${err.message}`);
    }
    return [];
  };

  // Security Middleware: Socket Authentication
  io.use(async (socket, next) => {
    const token = socket.handshake.auth.token;
    
    if (!token && process.env.NODE_ENV === 'production') {
      return next(new Error('Authentication error: Token missing'));
    }

    if (token && auth) {
      try {
        const decodedToken = await auth.verifyIdToken(token);
        socket.user = decodedToken;
        logger.info(`🔐 Authenticated: ${decodedToken.email}`);
        return next();
      } catch (error) {
        logger.error('❌ Socket Auth Error:', error.message);
        return next(new Error('Authentication error: Invalid token'));
      }
    }

    // Allow in dev without token if not configured
    next();
  });

  io.on('connection', (socket) => {
    logger.info(`🔌 New connection: ${socket.id} (${socket.user?.email || 'Anonymous'})`);

    // Dynamic Room Joining based on role
    socket.on('register', async (data) => {
      if (data.role === 'partner') {
        socket.join('partners');
        socket.partnerId = data.id; // Store partnerId on socket
        logger.info(`👷 Partner ${data.id} joined partners room`);
        await loadPartnerServices(data.id, socket);
      }
    });

    socket.on('updateLocation', async (data) => {
      const { partnerId, latitude, longitude, isOnline } = data;
      if (!partnerId) return;

      socket.partnerId = partnerId;
      socket.latitude = latitude;
      socket.longitude = longitude;
      socket.isOnline = isOnline;

      // Store in Redis using GEOADD for extremely fast geospatial querying
      try {
        if (isOnline && latitude && longitude) {
          // syntax: GEOADD key longitude latitude member
          await redis.geoadd('partners_location', longitude, latitude, partnerId);
          // Load services to cache on socket and Redis
          await loadPartnerServices(partnerId, socket);
          // Also update their metadata
          await redis.hset(`partner:${partnerId}`, 'isOnline', 'true', 'lastUpdated', Date.now());
        } else if (!isOnline) {
          // Remove from active locations if explicitly marked offline
          await redis.zrem('partners_location', partnerId);
          await redis.hset(`partner:${partnerId}`, 'isOnline', 'false');
        }
      } catch (redisError) {
        logger.warn(`⚠️ Failed to update partner location in Redis: ${redisError.message}`);
      }

      // Broadcast location to anyone tracking this partner (e.g., customers)
      io.to(`tracking_${partnerId}`).emit('partnerLocationUpdate', {
        partnerId,
        latitude,
        longitude,
        isOnline,
        timestamp: new Date()
      });
    });

    // Room for customers to track a specific partner
    socket.on('joinTracking', (partnerId) => {
      socket.join(`tracking_${partnerId}`);
      logger.info(`👥 Client ${socket.id} started tracking partner ${partnerId}`);
    });

    // Partner accepts a booking
    socket.on('acceptBooking', async (data) => {
      const { bookingId, partnerId, partnerName } = data;
      
      if (!db) return;

      try {
        const bookingRef = db.collection('bookings').doc(bookingId);
        
        // Use a transaction to ensure only one partner can accept
        await db.runTransaction(async (t) => {
          const doc = await t.get(bookingRef);
          if (!doc.exists) throw new Error('Booking not found');
          
          const bookingData = doc.data();
          if (bookingData.status !== 'searching') {
            throw new Error('Booking already accepted by someone else');
          }

          t.update(bookingRef, {
            status: 'accepted',
            workerId: partnerId,
            workerName: partnerName,
            acceptedAt: new Date(),
          });
        });

        logger.info(`✅ Booking ${bookingId} accepted by Partner ${partnerId}`);
        socket.emit('bookingAcceptedSuccess', { bookingId });
        
        // Notify the customer (if they are in the tracking room)
        io.to(`tracking_${partnerId}`).emit('partnerAssigned', { 
          partnerId, 
          partnerName 
        });

      } catch (error) {
        logger.error(`❌ Accept Booking Error: ${error.message}`);
        socket.emit('acceptBookingError', { message: error.message });
      }
    });

    // Customer requests a booking (Matchmaking)
    socket.on('requestBooking', async (data) => {
      const { bookingId, latitude, longitude, radiusInKm, category } = data;
      logger.info(`🔍 Customer ${socket.id} requesting booking ${bookingId} near ${latitude}, ${longitude} (Category: ${category})`);

      try {
        let specialization = category;
        if (!specialization && bookingId && db) {
          try {
            const bookingDoc = await db.collection('bookings').doc(bookingId).get();
            if (bookingDoc.exists) {
              const bookingData = bookingDoc.data();
              specialization = bookingData.category || bookingData.service;
            }
          } catch (dbErr) {
            logger.warn(`⚠️ Failed to fetch booking details for matchmaking: ${dbErr.message}`);
          }
        }

        // Normalize the specialization
        const normalizedSpec = normalizeSpecialization(specialization);
        if (specialization && !normalizedSpec) {
          logger.warn(`⚠️ Could not normalize specialization: "${specialization}". Proceeding without filtering.`);
        }

        const nearestPartners = await findNearestPartners(latitude, longitude, radiusInKm || 5, 10, normalizedSpec);
        
        if (nearestPartners.length === 0) {
          socket.emit('noPartnersFound', { bookingId, message: 'No partners available nearby right now.' });
          return;
        }

        logger.info(`✨ Found ${nearestPartners.length} nearby partners for booking ${bookingId} (Specialization: ${normalizedSpec})`);

        // Broadcast a 'newBookingRequest' to the specific socket IDs of those partners
        // To do this reliably, we can have partners join a room with their partnerId
        // or emit to all partners and let them filter, but emitting to specific rooms is better.
        nearestPartners.forEach(partner => {
          // partner.partnerId is their ID. Assuming they joined a room like 'partner_room_${partnerId}'
          // or we can emit to the general 'partners' room and include their IDs
          io.to('partners').emit('newBookingRequest', {
            bookingId,
            ...data,
            targetPartnerIds: nearestPartners.map(p => p.partnerId), // The client checks if its ID is in this list
            distances: nearestPartners
          });
        });

      } catch (error) {
        logger.error(`❌ requestBooking error: ${error.message}`);
        socket.emit('bookingError', { message: 'Failed to process booking request' });
      }
    });

    socket.on('disconnect', async (reason) => {
      if (socket.partnerId) {
        // Remove from active locations when disconnected
        try {
          await redis.zrem('partners_location', socket.partnerId);
          await redis.hset(`partner:${socket.partnerId}`, 'isOnline', 'false');
          logger.info(`🔴 Partner ${socket.partnerId} went offline (Disconnected)`);
        } catch (err) {
          logger.error(`❌ Redis error on disconnect: ${err.message}`);
        }
      }
      logger.info(`👋 Disconnected: ${socket.id} Reason: ${reason}`);
    });
  });

  return io;
};
