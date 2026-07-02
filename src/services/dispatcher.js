import { db } from '../config/firebase.js';
import logger from '../config/logger.js';
import { normalizeSpecialization } from './matchmaking.js';

// Haversine formula — distance between two GPS points in km
function getDistanceKm(lat1, lon1, lat2, lon2) {
  const R = 6371; // Earth's radius in km
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

export const startDispatcher = (io) => {
  if (!db) {
    logger.error('❌ Firestore not initialized. Dispatcher cannot start.');
    return;
  }

  logger.info('🛰️ Booking Dispatcher started...');

  // Listen for new bookings with status 'searching'
  db.collection('bookings')
    .where('status', '==', 'searching')
    .onSnapshot((snapshot) => {
      snapshot.docChanges().forEach((change) => {
        if (change.type === 'added' || change.type === 'modified') {
          const booking = { id: change.doc.id, ...change.doc.data() };
          
          // Only notify if it's still searching and hasn't been notified yet (optional throttle)
          if (booking.status === 'searching' && !booking.workerId) {
            const isUrgent = booking.items?.some(item => item.isUrgent) || !!booking.isUrgent;
            
            const bookingLat = booking.location?.lat || booking.latitude;
            const bookingLng = booking.location?.lng || booking.longitude;

            const bookingCategory = booking.category || booking.serviceName || (booking.items && booking.items[0]?.serviceName) || booking.service;
            const bookingSpecialization = normalizeSpecialization(bookingCategory);

            if (bookingLat && bookingLng) {
              logger.info(`🆕 New booking found: ${booking.id} (Category: ${bookingSpecialization}, Urgent: ${isUrgent}). Filtering partners within 2km...`);
              
              // Get all sockets in the 'partners' room
              const partnerSockets = io.sockets.adapter.rooms.get('partners');
              if (partnerSockets) {
                for (const socketId of partnerSockets) {
                  const socket = io.sockets.sockets.get(socketId);
                  if (socket && socket.latitude && socket.longitude) {
                    // Check if partner offers this service category (specialization)
                    const partnerServices = socket.selectedServices || [];
                    if (bookingSpecialization && partnerServices.length > 0) {
                      const hasSpecialization = partnerServices.some(s => s.toLowerCase() === bookingSpecialization.toLowerCase());
                      if (!hasSpecialization) {
                        logger.info(`⏭️ Skipping dispatch for booking ${booking.id} to partner ${socket.partnerId} (Mismatch specialization. Booking: ${bookingSpecialization}, Partner has: ${partnerServices.join(', ')})`);
                        continue;
                      }
                    }

                    const distance = getDistanceKm(bookingLat, bookingLng, socket.latitude, socket.longitude);
                    if (distance <= 2.0) { // 2 km operating circle limit
                      logger.info(`📌 Dispatching booking ${booking.id} to partner ${socket.partnerId} (${distance.toFixed(2)} km away)`);
                      socket.emit('newNearbyBooking', {
                        id: booking.id,
                        serviceName: booking.serviceName || (booking.items && booking.items[0]?.serviceName) || 'Service Request',
                        location: booking.location,
                        userAddress: booking.userAddress,
                        isUrgent: isUrgent,
                        distance: distance.toFixed(2), // Include exact distance
                        timestamp: new Date()
                      });

                      // Also emit 'newBookingRequest' to match partner app's expected listener
                      socket.emit('newBookingRequest', {
                        bookingId: booking.id,
                        serviceName: booking.serviceName || (booking.items && booking.items[0]?.serviceName) || 'Service Request',
                        location: booking.location,
                        userAddress: booking.userAddress,
                        isUrgent: isUrgent,
                        targetPartnerIds: [socket.partnerId],
                        distances: [{
                          partnerId: socket.partnerId,
                          distance: distance
                        }],
                        timestamp: new Date()
                      });
                    }
                  }
                }
              }
            } else {
              logger.info(`🆕 New booking found: ${booking.id} (Category: ${bookingSpecialization}, Urgent: ${isUrgent}). No coords, dispatching to matching partners...`);
              
              const partnerSockets = io.sockets.adapter.rooms.get('partners');
              if (partnerSockets) {
                for (const socketId of partnerSockets) {
                  const socket = io.sockets.sockets.get(socketId);
                  if (socket) {
                    const partnerServices = socket.selectedServices || [];
                    if (bookingSpecialization && partnerServices.length > 0) {
                      const hasSpecialization = partnerServices.some(s => s.toLowerCase() === bookingSpecialization.toLowerCase());
                      if (!hasSpecialization) continue;
                    }

                    socket.emit('newNearbyBooking', {
                      id: booking.id,
                      serviceName: booking.serviceName || (booking.items && booking.items[0]?.serviceName) || 'Service Request',
                      location: booking.location,
                      userAddress: booking.userAddress,
                      isUrgent: isUrgent,
                      distance: "unknown",
                      timestamp: new Date()
                    });

                    socket.emit('newBookingRequest', {
                      bookingId: booking.id,
                      serviceName: booking.serviceName || (booking.items && booking.items[0]?.serviceName) || 'Service Request',
                      location: booking.location,
                      userAddress: booking.userAddress,
                      isUrgent: isUrgent,
                      targetPartnerIds: [socket.partnerId],
                      distances: [],
                      timestamp: new Date()
                    });
                  }
                }
              }
            }
          }
        }
      });
    }, (error) => {
      logger.error('❌ Dispatcher Snapshot Error:', error);
    });
};
