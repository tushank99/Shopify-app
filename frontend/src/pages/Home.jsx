import { Link, useParams } from "react-router-dom";
import { useGetProductsQuery, useGetTopProductsQuery, useGetNewProductsQuery } from "../redux/api/productApiSlice";
import Loader from "../components/Loader";
import Message from "../components/Message";
import { FaArrowRight, FaStar, FaShippingFast, FaShieldAlt, FaHeadset } from "react-icons/fa";
import HeartIcon from "./Products/HeartIcon";
import PropTypes from "prop-types";
import { getImageUrl, handleImageError } from "../Utils/imageUtils";
import PersonalizedRecommendations from "./Products/PersonalizedRecommendations";

const Home = () => {
  const { keyword } = useParams();
  const { data, isLoading, isError, error } = useGetProductsQuery({ keyword });
  const { data: topProducts } = useGetTopProductsQuery();
  const { data: newProducts } = useGetNewProductsQuery();

  return (
    <div className="ml-[5%] min-h-screen bg-gray-900">
      {/* Hero Section */}
      <section className="relative bg-gradient-to-r from-blue-600 via-purple-600 to-pink-500 overflow-hidden">
        <div className="max-w-7xl mx-auto px-6 py-16 lg:py-24">
          <div className="grid lg:grid-cols-2 gap-12 items-center">
            <div className="text-white space-y-6">
              <h1 className="text-4xl lg:text-6xl font-bold leading-tight">
                Shop What <br />
                <span className="text-yellow-300">You See</span>
              </h1>
              <p className="text-lg text-gray-100 max-w-md">
                Discover amazing products at unbeatable prices. Quality you can trust, delivered to your doorstep.
              </p>
              <div className="flex gap-4">
                <Link
                  to="/shop"
                  className="bg-white text-purple-600 font-bold py-3 px-8 rounded-full hover:bg-yellow-300 hover:text-purple-700 transition-all duration-300 flex items-center gap-2"
                >
                  Explore Now <FaArrowRight />
                </Link>
              </div>
            </div>
            {topProducts && topProducts[0] && (
              <div className="hidden lg:block">
                <div className="relative">
                  <div className="absolute -inset-4 bg-white/20 rounded-3xl blur-xl"></div>
                  <img
                    src={getImageUrl(topProducts[0].image)}
                    alt="Featured Product"
                    onError={handleImageError}
                    className="relative w-full max-w-md mx-auto rounded-2xl shadow-2xl transform hover:scale-105 transition-transform duration-500"
                  />
                  <div className="absolute bottom-4 left-4 right-4 bg-black/70 backdrop-blur-sm rounded-xl p-4">
                    <p className="text-white font-semibold">{topProducts[0].name}</p>
                    <p className="text-yellow-400 font-bold text-xl">₹{topProducts[0].price}</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </section>

      {/* Trust Badges */}
      <section className="bg-gray-800 py-6 border-b border-gray-700">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="flex items-center gap-3 text-gray-300">
              <FaShippingFast className="text-2xl text-blue-400" />
              <div>
                <p className="font-semibold text-white">Free Shipping</p>
                <p className="text-sm">On orders over $50</p>
              </div>
            </div>
            <div className="flex items-center gap-3 text-gray-300">
              <FaShieldAlt className="text-2xl text-green-400" />
              <div>
                <p className="font-semibold text-white">Secure Payment</p>
                <p className="text-sm">100% protected</p>
              </div>
            </div>
            <div className="flex items-center gap-3 text-gray-300">
              <FaHeadset className="text-2xl text-purple-400" />
              <div>
                <p className="font-semibold text-white">24/7 Support</p>
                <p className="text-sm">Ready to help</p>
              </div>
            </div>
            <div className="flex items-center gap-3 text-gray-300">
              <FaStar className="text-2xl text-yellow-400" />
              <div>
                <p className="font-semibold text-white">Top Quality</p>
                <p className="text-sm">Best products</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Categories Quick Links */}
      <section className="py-10 bg-gray-900">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex justify-center gap-4 flex-wrap">
            {["Electronics", "Clothing", "Home & Garden"].map((category) => (
              <Link
                key={category}
                to="/shop"
                className="bg-gray-800 text-white px-6 py-3 rounded-full border border-gray-700 hover:border-pink-500 hover:bg-gray-700 transition-all duration-300"
              >
                {category}
              </Link>
            ))}
          </div>
        </div>
      </section>

      
      <PersonalizedRecommendations />

      

      {isLoading ? (
        <div className="flex justify-center py-20">
          <Loader />
        </div>
      ) : isError ? (
        <Message variant="danger">
          {error?.data?.message || error?.error || "Failed to load products."}
        </Message>
      ) : (
        <>
          {/* Top Rated Products */}
          {topProducts && topProducts.length > 0 && (
            <section className="py-12 bg-gray-900">
              <div className="max-w-7xl mx-auto px-6">
                <div className="flex justify-between items-center mb-8">
                  <div>
                    <h2 className="text-2xl lg:text-3xl font-bold text-white">
                      ⭐ Top Rated Products
                    </h2>
                    <p className="text-gray-400 mt-1">Loved by our customers</p>
                  </div>
                  <Link
                    to="/shop"
                    className="text-pink-400 hover:text-pink-300 font-semibold flex items-center gap-2"
                  >
                    View All <FaArrowRight />
                  </Link>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                  {topProducts.slice(0, 4).map((product) => (
                    <ProductCard key={product._id} product={product} />
                  ))}
                </div>
              </div>
            </section>
          )}

          {/* New Arrivals */}
          {newProducts && newProducts.length > 0 && (
            <section className="py-12 bg-gray-800">
              <div className="max-w-7xl mx-auto px-6">
                <div className="flex justify-between items-center mb-8">
                  <div>
                    <h2 className="text-2xl lg:text-3xl font-bold text-white">
                      🆕 New Arrivals
                    </h2>
                    <p className="text-gray-400 mt-1">Fresh products just added</p>
                  </div>
                  <Link
                    to="/shop"
                    className="text-pink-400 hover:text-pink-300 font-semibold flex items-center gap-2"
                  >
                    View All <FaArrowRight />
                  </Link>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                  {newProducts.slice(0, 4).map((product) => (
                    <ProductCard key={product._id} product={product} />
                  ))}
                </div>
              </div>
            </section>
          )}

          {/* All Products */}
          <section className="py-12 bg-gray-900">
            <div className="max-w-7xl mx-auto px-6">
              <div className="flex justify-between items-center mb-8">
                <div>
                  <h2 className="text-2xl lg:text-3xl font-bold text-white">
                    🛍️ All Products
                  </h2>
                  <p className="text-gray-400 mt-1">Browse our complete collection</p>
                </div>
                <Link
                  to="/shop"
                  className="bg-pink-600 text-white font-bold py-2 px-6 rounded-full hover:bg-pink-500 transition-all duration-300 flex items-center gap-2"
                >
                  Shop Now <FaArrowRight />
                </Link>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                {data?.products?.map((product) => (
                  <ProductCard key={product._id} product={product} />
                ))}
              </div>
            </div>
          </section>
        </>
      )}

      {/* Footer Banner */}
      <section className="bg-gradient-to-r from-pink-600 to-purple-600 py-12">
        <div className="max-w-7xl mx-auto px-6 text-center">
          <h2 className="text-3xl font-bold text-white mb-4">
            Ready to Start Shopping?
          </h2>
          <p className="text-gray-100 mb-6 max-w-2xl mx-auto">
            Join thousands of happy customers and discover the best deals on quality products.
          </p>
          <Link
            to="/shop"
            className="inline-block bg-white text-purple-600 font-bold py-3 px-10 rounded-full hover:bg-yellow-300 transition-all duration-300"
          >
            Start Shopping
          </Link>
        </div>
      </section>
    </div>
  );
};

// Product Card Component
const ProductCard = ({ product }) => {
  return (
    <div className="bg-gray-800 rounded-xl overflow-hidden border border-gray-700 hover:border-pink-500 transition-all duration-300 group">
      <div className="relative overflow-hidden">
        <Link to={`/product/${product._id}`}>
          <img
            src={getImageUrl(product.image)}
            alt={product.name}
            onError={handleImageError}
            className="w-full h-56 object-cover group-hover:scale-110 transition-transform duration-500"
          />
        </Link>
        <div className="absolute top-3 right-3">
          <HeartIcon product={product} />
        </div>
        {product.countInStock === 0 && (
          <div className="absolute top-3 left-3 bg-red-500 text-white text-xs font-bold px-2 py-1 rounded">
            Out of Stock
          </div>
        )}
      </div>
      <div className="p-4">
        <Link to={`/product/${product._id}`}>
          <h3 className="text-white font-semibold text-lg mb-2 truncate hover:text-pink-400 transition-colors">
            {product.name}
          </h3>
        </Link>
        <div className="flex items-center justify-between">
          <span className="text-2xl font-bold text-pink-400">₹{product.price}</span>
          <div className="flex items-center gap-1 text-yellow-400">
            <FaStar />
            <span className="text-gray-300 text-sm">{product.rating?.toFixed(1) || "N/A"}</span>
          </div>
        </div>
        <p className="text-gray-400 text-sm mt-2">{product.brand}</p>
      </div>
    </div>
  );
};

ProductCard.propTypes = {
  product: PropTypes.shape({
    _id: PropTypes.string.isRequired,
    name: PropTypes.string.isRequired,
    image: PropTypes.string.isRequired,
    price: PropTypes.number.isRequired,
    brand: PropTypes.string,
    rating: PropTypes.number,
    countInStock: PropTypes.number,
  }).isRequired,
};

export default Home;
