import redis from '../config/redis.js';
import logger from '../config/logger.js';
import { db } from '../config/firebase.js';

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

/**
 * Normalizes a service name or category string into a standard specialization category.
 * @param {string} serviceOrCategory 
 * @returns {string|null}
 */
export const normalizeSpecialization = (serviceOrCategory) => {
  if (!serviceOrCategory) return null;
  const normalized = serviceOrCategory.toLowerCase().trim();
  
  // Exact or direct matches
  if (normalized === 'cleaning') return "Cleaning";
  if (normalized === 'ac repair') return "AC Repair";
  if (normalized === 'plumbing') return "Plumbing";
  if (normalized === 'electrician') return "Electrician";
  if (normalized === 'salon') return "Salon";

  // Keyword matches for Electrician
  if (normalized.includes("elect") || normalized.includes("fan") || normalized.includes("light") || normalized.includes("switch") || normalized.includes("wire")) {
    return "Electrician";
  }
  // Keyword matches for Plumbing
  if (normalized.includes("plumb") || normalized.includes("pipe") || normalized.includes("tap") || normalized.includes("fitting") || normalized.includes("leak") || normalized.includes("drain") || normalized.includes("sink")) {
    return "Plumbing";
  }
  // Keyword matches for AC Repair
  if (normalized.includes("ac") || normalized.includes("cool")) {
    return "AC Repair";
  }
  // Keyword matches for Salon
  if (normalized.includes("salon") || normalized.includes("groom") || normalized.includes("spa") || normalized.includes("hair") || normalized.includes("facial") || normalized.includes("massage")) {
    return "Salon";
  }
  // Keyword matches for Cleaning (check cleaning last as fallback for house, deep, etc.)
  if (normalized.includes("clean") || normalized.includes("wash") || normalized.includes("sofa") || normalized.includes("carpet") || normalized.includes("deep") || normalized.includes("fridge")) {
    return "Cleaning";
  }

  return null;
};

/**
 * Finds the nearest active partners to a given location using Redis GEORADIUS.
 * Falls back to Firestore if Redis is unavailable.
 * @param {number} latitude 
 * @param {number} longitude 
 * @param {number} radiusInKm - Search radius in kilometers
 * @param {number} maxResults - Maximum number of partners to return
 * @param {string|null} specialization - Required partner specialization
 * @returns {Promise<Array<{partnerId: string, distance: number}>>}
 */
export const findNearestPartners = async (latitude, longitude, radiusInKm = 5, maxResults = 10, specialization = null) => {
  try {
    let results = [];
    
    try {
      // Try to query Redis first
      results = await redis.georadius(
        'partners_location',
        longitude,
        latitude,
        radiusInKm,
        'km',
        'WITHDIST',
        'WITHCOORD',
        'ASC',
        'COUNT',
        specialization ? Math.max(maxResults * 5, 50) : maxResults
      );

      // Filter by specialization if requested
      if (specialization) {
        const filteredResults = [];
        const pipeline = redis.pipeline();
        results.forEach(r => {
          pipeline.hget(`partner:${r[0]}`, 'selectedServices');
        });
        const specializations = await pipeline.exec();

        for (let i = 0; i < results.length; i++) {
          const partnerId = results[i][0];
          const servicesRaw = specializations[i] ? specializations[i][1] : null;
          let services = [];

          if (servicesRaw) {
            try {
              services = JSON.parse(servicesRaw);
            } catch (err) {
              logger.warn(`⚠️ Failed to parse selectedServices for partner ${partnerId}: ${err.message}`);
            }
          } else if (db) {
            // Fallback: fetch from Firestore if Redis cache doesn't have it
            try {
              const partnerDoc = await db.collection('partners').doc(partnerId).get();
              if (partnerDoc.exists) {
                services = partnerDoc.data().selectedServices || [];
                // Cache it back to Redis
                await redis.hset(`partner:${partnerId}`, 'selectedServices', JSON.stringify(services));
              }
            } catch (fsErr) {
              logger.warn(`⚠️ Firestore fallback failed for partner ${partnerId}: ${fsErr.message}`);
            }
          }

          if (services.some(s => s.toLowerCase() === specialization.toLowerCase())) {
            filteredResults.push(results[i]);
          }
        }
        results = filteredResults.slice(0, maxResults);
      }
    } catch (redisError) {
      logger.warn(`⚠️ Redis georadius failed, falling back to Firestore: ${redisError.message}`);
      
      // Fallback: Query all online partners from Firestore and filter locally
      if (db) {
        let queryRef = db.collection('partners').where('isOnline', '==', true);
        if (specialization) {
          queryRef = queryRef.where('selectedServices', 'array-contains', specialization);
        }
        const snapshot = await queryRef.get();
        
        const onlinePartners = [];
        snapshot.forEach(doc => {
          const data = doc.data();
          const pLat = data.latitude || data.location?.lat;
          const pLng = data.longitude || data.location?.lng;
          if (pLat && pLng) {
            const distance = getDistanceKm(latitude, longitude, pLat, pLng);
            if (distance <= radiusInKm) {
              onlinePartners.push({
                partnerId: doc.id,
                distance,
                coords: [pLng, pLat]
              });
            }
          }
        });
        
        // Sort by distance ASC and take maxResults
        onlinePartners.sort((a, b) => a.distance - b.distance);
        const limitedPartners = onlinePartners.slice(0, maxResults);
        
        // Format to match redis.georadius result structure: [partnerId, distanceStr, [lon, lat]]
        results = limitedPartners.map(p => [
          p.partnerId,
          p.distance.toFixed(3),
          p.coords
        ]);
      } else {
        throw new Error('Firestore database is not initialized, cannot perform matchmaking fallback');
      }
    }

    // Fetch real driving distance from OSRM for each partner
    const partners = await Promise.all(results.map(async (result) => {
      const partnerId = result[0];
      const straightLineDist = parseFloat(result[1]);
      const [partnerLon, partnerLat] = result[2];

      let distance = straightLineDist;
      let etaMinutes = null;

      try {
        // Use 'bike' profile for shortest urban routes instead of 'driving' which takes long highway detours
        const osrmUrl = `http://router.project-osrm.org/route/v1/bike/${partnerLon},${partnerLat};${longitude},${latitude}?overview=false`;
        const response = await fetch(osrmUrl);
        const data = await response.json();

        if (data.code === 'Ok' && data.routes && data.routes.length > 0) {
          distance = data.routes[0].distance / 1000;
          // Calculate realistic city ETA based on 25 km/h average speed, since 'bike' duration would be too slow
          etaMinutes = Math.ceil((distance / 25) * 60);
        }
      } catch (err) {
        logger.warn(`⚠️ OSRM API failed for partner ${partnerId}, falling back to straight-line distance.`);
      }

      return {
        partnerId,
        distance,
        eta: etaMinutes // e.g. 5 (minutes)
      };
    }));

    // Re-sort by actual driving distance just in case
    partners.sort((a, b) => a.distance - b.distance);

    return partners;
  } catch (error) {
    logger.error(`❌ Matchmaking Error (findNearestPartners): ${error.message}`);
    throw error;
  }
};
