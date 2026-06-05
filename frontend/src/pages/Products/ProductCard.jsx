import { Link } from "react-router-dom";
import { AiOutlineShoppingCart } from "react-icons/ai";
import { FaStar } from "react-icons/fa";
import { useDispatch } from "react-redux";
import { addToCart } from "../../redux/features/cart/cartSlice";
import { toast } from "react-toastify";
import PropTypes from "prop-types";
import HeartIcon from "./HeartIcon";
import { getImageUrl, handleImageError } from "../../Utils/imageUtils";

const ProductCard = ({ p }) => {
  const dispatch = useDispatch();

  const addToCartHandler = (product, qty) => {
    dispatch(addToCart({ ...product, qty }));
    toast.success("Item added successfully", {
      position: toast.POSITION.TOP_RIGHT,
      autoClose: 2000,
    });
  };

  return (
    <div className="bg-gray-800 rounded-xl overflow-hidden border border-gray-700 hover:border-pink-500 transition-all duration-300 h-full flex flex-col">
      {/* Image Section */}
      <div className="relative overflow-hidden group">
        <Link to={`/product/${p._id}`}>
          <img
            className="w-full h-48 object-cover group-hover:scale-110 transition-transform duration-500"
            src={getImageUrl(p.image)}
            alt={p.name}
            onError={handleImageError}
          />
        </Link>
        {/* Brand Badge */}
        <span className="absolute bottom-3 right-3 bg-pink-600/90 text-white text-xs font-semibold px-2.5 py-1 rounded-full backdrop-blur-sm">
          {p?.brand}
        </span>
        {/* Heart Icon */}
        <div className="absolute top-3 right-3">
          <HeartIcon product={p} />
        </div>
        {/* Out of Stock */}
        {p?.countInStock === 0 && (
          <div className="absolute top-3 left-3 bg-red-500 text-white text-xs font-bold px-2 py-1 rounded">
            Out of Stock
          </div>
        )}
      </div>

      {/* Content Section */}
      <div className="p-4 flex flex-col flex-grow">
        {/* Title & Price */}
        <div className="flex justify-between items-start mb-2">
          <Link to={`/product/${p._id}`}>
            <h5 className="text-lg font-semibold text-white hover:text-pink-400 transition-colors line-clamp-1">
              {p?.name}
            </h5>
          </Link>
          <p className="font-bold text-pink-400 text-lg ml-2 whitespace-nowrap">
            ₹{(p?.price?? 0).toFixed(2)}
          </p>
        </div>

        {/* Rating */}
        <div className="flex items-center gap-1 mb-3">
          <FaStar className="text-yellow-400" size={14} />
          <span className="text-gray-400 text-sm">
            {(p?.rating?? 0).toFixed(1)} ({p?.numReviews || 0} reviews)
          </span>
        </div>

        {/* Description */}
        <p className="text-gray-400 text-sm mb-4 line-clamp-2 flex-grow">
          {p?.description?.substring(0, 80)}...
        </p>

        {/* Actions */}
        <div className="flex gap-2 mt-auto">
          <Link
            to={`/product/${p._id}`}
            className="flex-1 inline-flex items-center justify-center px-4 py-2.5 text-sm font-medium text-white bg-pink-600 rounded-lg hover:bg-pink-500 transition-colors duration-200"
          >
            View Details
          </Link>

          <button
            className="p-2.5 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors duration-200 text-white"
            onClick={() => addToCartHandler(p, 1)}
            disabled={p?.countInStock === 0}
          >
            <AiOutlineShoppingCart size={20} />
          </button>
        </div>
      </div>
    </div>
  );
};

ProductCard.propTypes = {
  p: PropTypes.shape({
    _id: PropTypes.string.isRequired,
    name: PropTypes.string,
    image: PropTypes.string,
    brand: PropTypes.string,
    price: PropTypes.number,
    description: PropTypes.string,
    rating: PropTypes.number,
    numReviews: PropTypes.number,
    countInStock: PropTypes.number,
  }).isRequired,
};

export default ProductCard;
